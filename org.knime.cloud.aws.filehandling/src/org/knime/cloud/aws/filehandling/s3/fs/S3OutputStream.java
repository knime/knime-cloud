/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 *
 * History
 *   2022-02-14 (Alexander Bondaletov): created
 */
package org.knime.cloud.aws.filehandling.s3.fs;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.knime.cloud.aws.filehandling.s3.AwsUtils;
import org.knime.cloud.aws.filehandling.s3.MultiRegionS3Client;
import org.knime.cloud.aws.filehandling.s3.fs.api.S3FSConnectionConfig;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.util.FileUtil;
import org.knime.filehandling.core.connections.FSFiles;
import org.knime.filehandling.core.defaultnodesettings.ExceptionUtil;

import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.internal.util.Mimetype;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

/**
 * {@link OutputStream} implementation for the S3 storage. Uses Multipart upload for files larger than
 * multipartUploadPartSize specified in the {@link S3FSConnectionConfig}.
 *
 * @author Alexander Bondaletov
 */
public class S3OutputStream extends OutputStream {

    private static final NodeLogger LOG = NodeLogger.getLogger(S3OutputStream.class);

    private static final int MINIMUM_PART_SIZE = 5 * 1024 * 1024; // 5 MB

    private final S3Path m_path;

    private final String m_mimeType;

    private final MultiRegionS3Client m_client;

    private final ExecutorService m_executor;

    private final int m_maxPartSize;

    private final Path[] m_tempFiles;

    private final List<Future<CompletedPart>> m_uploadedParts;

    private boolean m_isOpen;

    private int m_currentTempFileIdx;

    private SeekableByteChannel m_currentChannel;

    private int m_currentChannelBytesWritten;

    private String m_uploadId;

    /**
     * @param path The file path.
     * @throws IOException
     *
     */
    @SuppressWarnings("resource")
    public S3OutputStream(final S3Path path) throws IOException {
        m_path = path;
        m_mimeType = Mimetype.getInstance().getMimetype(m_path);
        m_client = path.getFileSystem().getClient();
        m_executor = Executors.newSingleThreadExecutor();

        m_maxPartSize = path.getFileSystem().getMultipartUploadPartSize();
        CheckUtils.checkArgument(m_maxPartSize >= MINIMUM_PART_SIZE,
                "Mutipart upload part size cannot be less than " + MINIMUM_PART_SIZE);

        m_isOpen = true;

        m_tempFiles = new Path[2];
        m_tempFiles[0] = FileUtil.createTempFile("s3-upload", "").toPath();
        m_tempFiles[1] = FileUtil.createTempFile("s3-upload", "").toPath();

        m_currentTempFileIdx = 0;
        m_currentChannel = Files.newByteChannel(m_tempFiles[m_currentTempFileIdx], StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
        m_currentChannelBytesWritten = 0;

        m_uploadedParts = new ArrayList<>();
    }

    @Override
    public void write(final int b) throws IOException {
        if (!m_isOpen) {
            throw new IOException("Stream already closed");
        }

        final var arr = new byte[1];
        arr[0] = (byte) b;
        write(arr, 0, 1);
    }

    @Override
    public void write(final byte[] src, final int off, final int len) throws IOException {
        if (!m_isOpen) {
            throw new IOException("Stream already closed");
        }

        final var byteBuffer = ByteBuffer.wrap(src, off, len);

        while (byteBuffer.remaining() > 0) {
            submitPartIfNecessary(false);
            int bytesToWrite = Math.min(byteBuffer.remaining(), m_maxPartSize - m_currentChannelBytesWritten);

            // potentially lower the limit of the byte buffer to respect part size
            byteBuffer.limit(byteBuffer.position() + bytesToWrite);

            writeByteBuffer(byteBuffer);
            m_currentChannelBytesWritten += bytesToWrite;
            byteBuffer.limit(off + len);
        }
    }

    private void writeByteBuffer(final ByteBuffer bb) throws IOException {
        while (bb.remaining() > 0) {
            m_currentChannel.write(bb);
        }
    }

    private void submitPartIfNecessary(final boolean flush) throws IOException {
        if (m_currentChannelBytesWritten == m_maxPartSize || (flush && m_currentChannelBytesWritten > 0)) {

            m_currentChannel.close();
            uploadPartFromCurrentTempFile();

            if (m_uploadedParts.size() > 1) {
                // wait until previous file is available for writing
                retrieveResult(m_uploadedParts.size() - 2);
            }
            switchCurrentTempFile();
        }
    }

    private void switchCurrentTempFile() throws IOException {
        m_currentTempFileIdx = m_currentTempFileIdx == 0 ? 1 : 0;
        m_currentChannel = Files.newByteChannel(m_tempFiles[m_currentTempFileIdx], StandardOpenOption.TRUNCATE_EXISTING,
            StandardOpenOption.WRITE);
        m_currentChannelBytesWritten = 0;
    }

    private void uploadPartFromCurrentTempFile() throws IOException {
        if (m_uploadId == null) {
            initializeMultipartUpload();
        }

        final var body = createRequestBody();
        final var partNumber = m_uploadedParts.size() + 1; //part numbers starts from 1

        var partFuture = m_executor.submit(() -> {
            final UploadPartResponse resp =
                m_client.uploadPart(m_path.getBucketName(), m_path.getBlobName(), m_uploadId, partNumber, body);

            return CompletedPart.builder().eTag(resp.eTag()).partNumber(partNumber).build();
        });

        m_uploadedParts.add(partFuture);
    }

    private RequestBody createRequestBody() {
        return RequestBody.fromFile(m_tempFiles[m_currentTempFileIdx]);
    }

    private void initializeMultipartUpload() throws IOException {
        try {
            m_uploadId =
                m_client.createMultipartUpload(m_path.getBucketName(), m_path.getBlobName(), m_mimeType).uploadId();
        } catch (SdkException e) {
            throw AwsUtils.toIOE(e, m_path);
        }
    }

    @Override
    public void flush() throws IOException {
        if (m_isOpen && m_currentChannelBytesWritten > MINIMUM_PART_SIZE && m_uploadId != null) {
            submitPartIfNecessary(true);
        }
    }

    @Override
    public void close() throws IOException {
        if (!m_isOpen) {
            return;
        }

        try {
            if (m_uploadId != null) {
                completeMultipartUpload();
            } else {
                uploadAsSingleRequest();
            }
        } finally {
            m_isOpen = false;
            cleanup();
        }
    }

    private void cleanup() {
        try {
            m_currentChannel.close();
        } catch (IOException e) { // NOSONAR can be ignored
        } finally {
            m_currentChannel = null;
        }

        m_executor.shutdownNow();
        FSFiles.deleteSafely(m_tempFiles[0]);
        FSFiles.deleteSafely(m_tempFiles[1]);
    }

    private void completeMultipartUpload() throws IOException {
        submitPartIfNecessary(true);
        final List<CompletedPart> parts = collectCompletedParts();
        try {
            m_client.completeMultipartUpload(m_path.getBucketName(), m_path.getBlobName(), m_uploadId, parts);
        } catch (SdkException ex) {
            abortMutipartUploadSafely();
            throw AwsUtils.toIOE(ex, m_path);
        }
    }

    private List<CompletedPart> collectCompletedParts() throws IOException {
        List<CompletedPart> result = new ArrayList<>();
        for (var i = 0; i < m_uploadedParts.size(); i++) {
            result.add(retrieveResult(i));
        }
        return result;
    }

    private CompletedPart retrieveResult(final int index) throws IOException {
        if (index >= m_uploadedParts.size()) {
            throw new IllegalArgumentException("invalid index");
        }
        try {
            return m_uploadedParts.get(index).get();
        } catch (ExecutionException ex) {//NOSONAR

            abortMutipartUploadSafely();


            if (ex.getCause() instanceof SdkException) {
                throw AwsUtils.toIOE((SdkException)ex.getCause(), m_path);
            } else {
                throw ExceptionUtil.wrapAsIOException(ex.getCause());
            }
        } catch (Exception ex) { //NOSONAR
            abortMutipartUploadSafely();
            throw ExceptionUtil.wrapAsIOException(ex);
        }
    }

    private void uploadAsSingleRequest() throws IOException {
        try {
            m_client.putObject(m_path.getBucketName(), m_path.getBlobName(), createRequestBody(), m_mimeType);
        } catch (SdkException e) {
            throw AwsUtils.toIOE(e, m_path);
        }
    }

    private void abortMutipartUploadSafely() {
        if (m_uploadId != null) {
            m_uploadedParts.forEach(future -> future.cancel(true));

            try {
                m_client.abortMultipartUpload(m_path.getBucketName(), m_path.getBlobName(), m_uploadId);
            } catch (Exception e) { // NOSONAR this method must not rethrow as it is used in catch blocks
                LOG.error("Failed to abort multipart upload", e);
            } finally {
                m_uploadedParts.clear();
                m_uploadId = null;
            }
        }
    }
}
