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
 *   20.08.2019 (Mareike Hoeger, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.cloud.aws.filehandling.s3.fs;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.knime.cloud.aws.filehandling.s3.AwsUtils;
import org.knime.cloud.aws.filehandling.s3.MultiRegionS3Client;
import org.knime.core.node.NodeLogger;
import org.knime.filehandling.core.connections.base.BaseFileSystemProvider;
import org.knime.filehandling.core.connections.base.attributes.BaseFileAttributes;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;

/**
 * File system provider for {@link S3FileSystem}s.
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
class S3FileSystemProvider extends BaseFileSystemProvider<S3Path, S3FileSystem> {

    @SuppressWarnings("unused")
    private static final NodeLogger LOGGER = NodeLogger.getLogger(S3FileSystemProvider.class);

    @Override
    protected SeekableByteChannel newByteChannelInternal(final S3Path path, final Set<? extends OpenOption> options,
        final FileAttribute<?>... attrs) throws IOException {
        return new S3SeekableByteChannel(path, options);
    }

    @SuppressWarnings("resource")
    @Override
    protected void createDirectoryInternal(final S3Path dir, final FileAttribute<?>... attrs) throws IOException {
        final String bucketName = dir.getBucketName();

        try {
            if (dir.getBlobName() != null) {
                final String objectKey = dir.toDirectoryPath().getBlobName();
                dir.getFileSystem().getClient().putObject(bucketName, objectKey, RequestBody.empty());
            } else {
                dir.getFileSystem().getClient().createBucket(bucketName);
            }
        } catch (SdkException e) {
            throw AwsUtils.toIOE(e, dir);
        }
    }

    @SuppressWarnings("resource")
    @Override
    protected void copyInternal(final S3Path source, final S3Path target, final CopyOption... options) throws IOException {
        final MultiRegionS3Client client = source.getFileSystem().getClient();

        if (!source.isDirectory()) {
            try {
                client.copyObject(source.getBucketName(), source.getBlobName(), target.getBucketName(),
                    target.getBlobName());
            } catch (final SdkException ex) {
                throw AwsUtils.toIOE(ex, source, target);
            }
        } else {
            if (!dirIsEmpty(target)) {
                throw new DirectoryNotEmptyException(
                    String.format("Target directory %s exists and is not empty", target.toString()));
            }
            createDirectory(target);
        }
    }

    @SuppressWarnings("resource")
    private static boolean dirIsEmpty(final S3Path dir) throws IOException {
        final MultiRegionS3Client client = dir.getFileSystem().getClient();

        final ListObjectsV2Request listRequest = ListObjectsV2Request.builder()//
            .bucket(dir.getBucketName())//
            .prefix(dir.getBlobName())//
            .delimiter(dir.getFileSystem().getSeparator())//
            .encodingType("url")//
            .startAfter(dir.getBlobName())//
            .maxKeys(1)//
            .build();

        try {
            return client.listObjects(listRequest).keyCount() == 0;
        } catch (SdkException e) {
            throw AwsUtils.toIOE(e, dir);
        }
    }

    @Override
    protected void checkAccessInternal(final S3Path s3Path, final AccessMode... modes) throws IOException {
        // do nothing, too complex
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("resource")
    @Override
    protected InputStream newInputStreamInternal(final S3Path path, final OpenOption... options) throws IOException {

        final ResponseInputStream<GetObjectResponse> inputStream;

        if (path.getBlobName() == null) {
            throw new IOException("Cannot open input stream on bucket.");
        }

        try {

            inputStream = path.getFileSystem().getClient().getObject(path.getBucketName(), path.getBlobName());

            if (inputStream == null) {
                throw new IOException(String.format("Could not read path %s", path));
            }

        } catch (SdkException ex) {
            throw AwsUtils.toIOE(ex, path);
        }

        if (inputStream.response().contentLength() == 0) {
            try {
                inputStream.close();
            } catch (Exception e) {} // NOSONAR

            return new ByteArrayInputStream(new byte[0]);
        } else {
            return inputStream;
        }
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("resource")
    @Override
    protected OutputStream newOutputStreamInternal(final S3Path path, final OpenOption... options) throws IOException {
        final int len = options.length;
        final Set<OpenOption> opts = new HashSet<>(len + 3);
        if (len == 0) {
            opts.add(StandardOpenOption.CREATE);
            opts.add(StandardOpenOption.TRUNCATE_EXISTING);
        } else {
            for (final OpenOption opt : options) {
                if (opt == StandardOpenOption.READ) {
                    throw new IllegalArgumentException("READ not allowed");
                }
                opts.add(opt);
            }
        }
        opts.add(StandardOpenOption.WRITE);

        if (opts.contains(StandardOpenOption.APPEND)) {
            return Channels.newOutputStream(newByteChannel(path, opts));
        } else {
            return new S3OutputStream(path);
        }
    }

    @Override
    protected Iterator<S3Path> createPathIterator(final S3Path dir, final Filter<? super Path> filter) throws IOException {
        return S3PathIteratorFactory.create(dir.toDirectoryPath(), filter);
    }

    @Override
    protected BaseFileAttributes fetchAttributesInternal(final S3Path path, final Class<?> type) throws IOException {
        if (path.getBlobName() != null) {
            return fetchAttributesForObjectPath(path);
        } else if (path.getBucketName() != null) {
            return fetchAttributesForBucket(path);
        } else {
            return new BaseFileAttributes(false, //
                path, //
                FileTime.fromMillis(0), //
                FileTime.fromMillis(0), //
                FileTime.fromMillis(0), //
                0L, //
                false, //
                false, //
                null);
        }
    }

    private static BaseFileAttributes fetchAttributesForBucket(final S3Path path) throws IOException {
        return createBucketFileAttributes(getBucket(path), path);
    }

    private static BaseFileAttributes fetchAttributesForObjectPath(final S3Path path) throws IOException {
        // first we try whether there is an object for the given path
        // (whether path.isDirectory() or not)
        BaseFileAttributes attributes = fetchAttributesForObject(path);

        // directory case (1): when given "/path", but it does not exist, then we check for /path/
        if (attributes == null && !path.isDirectory()) {
            attributes = fetchAttributesForObject(path.toDirectoryPath());
        }

        // directory case (2): last possibility, when neither "/path" nor "/path/" exist, it could be
        // that the "directory" is just the common prefix of some objects.
        if (attributes == null) {
            attributes = fetchAttributesForPrefix(path);
        }

        if (attributes != null) {
            return attributes;
        } else {
            throw new NoSuchFileException(path.toString());
        }

    }

    @SuppressWarnings("resource")
    private static BaseFileAttributes fetchAttributesForObject(final S3Path path) throws IOException {
        try {
            HeadObjectResponse metadata =
                path.getFileSystem().getClient().headObject(path.getBucketName(), path.getBlobName());
            if (metadata != null) {
                return convertMetaDataToFileAttributes(path, metadata);
            } else {
                return null;
            }
        } catch (SdkException e) {
            throw AwsUtils.toIOE(e, path);
        }
    }

    @SuppressWarnings("resource")
    private static BaseFileAttributes fetchAttributesForPrefix(final S3Path path) throws IOException {
        S3Path dirPath = path.toDirectoryPath();

        final ListObjectsV2Request request = ListObjectsV2Request.builder()//
            .bucket(dirPath.getBucketName())//
            .prefix(dirPath.getBlobName())//
            .startAfter(dirPath.getBlobName())//
            .maxKeys(1)//
            .build();

        try {
            final ListObjectsV2Response result = path.getFileSystem().getClient().listObjects(request);

            if (!result.contents().isEmpty() || !result.commonPrefixes().isEmpty()) {
                return new BaseFileAttributes(false, //
                    path, //
                    FileTime.fromMillis(0), //
                    FileTime.fromMillis(0), //
                    FileTime.fromMillis(0), //
                    0L, //
                    false, //
                    false, //
                    null);
            } else {
                return null;
            }
        } catch (SdkException e) {
            throw AwsUtils.toIOE(e, path);
        }
    }

    private static BaseFileAttributes convertMetaDataToFileAttributes(final S3Path path,
        final HeadObjectResponse objectMetadata) throws IOException {
        final FileTime lastMod = determineObjectLastModificationTime(path, objectMetadata);

        long size = 0;
        if (!path.isDirectory()) {
            size = objectMetadata.contentLength();
        }

        return new BaseFileAttributes(!path.isDirectory(), path, //
            lastMod, //
            lastMod, //
            lastMod, //
            size, //
            false, //
            false, //
            null);
    }

    /**
     * @param path
     * @param objectMetadata
     * @return
     * @throws IOException
     */
    private static FileTime determineObjectLastModificationTime(final S3Path path, final HeadObjectResponse objectMetadata)
        throws IOException {
        final FileTime lastMod;
        if (objectMetadata.lastModified() != null) {
            lastMod = FileTime.from(objectMetadata.lastModified());
        } else {
            final Bucket bucket = getBucket(path);

            if (bucket.creationDate() != null) {
                lastMod = FileTime.from(bucket.creationDate());
            } else {
                lastMod = FileTime.fromMillis(0);
            }
        }
        return lastMod;
    }

    @SuppressWarnings("resource")
    private static Bucket getBucket(final S3Path path) throws IOException {
        try {
            Bucket bucket = path.getFileSystem().getClient().getBucket(path.getBucketName());

            if(bucket == null) {
                throw new NoSuchFileException(path.toString());
            }

            return bucket;
        } catch (SdkException ex) {
            throw AwsUtils.toIOE(ex, path);
        }
    }

    @SuppressWarnings("resource")
    @Override
    protected void deleteInternal(final S3Path path) throws IOException {
        try {
            final MultiRegionS3Client client = path.getFileSystem().getClient();

            if (path.getBlobName() != null) {
                if (!path.isDirectory() && client.doesObjectExist(path.getBucketName(), path.getBlobName())) {
                    // regular file. deleteObject does not fail if the object has been deleted in the meantime
                    client.deleteObject(path.getBucketName(), path.getBlobName());
                } else {
                    deleteFolder(path);
                }
            } else {
                client.deleteBucket(path.getBucketName());
            }
        } catch (final SdkException ex) {
            throw AwsUtils.toIOE(ex, path);
        }
    }

    @SuppressWarnings("resource")
    private static void deleteFolder(final S3Path origPath) throws IOException {
        final S3Path dirPath = origPath.toDirectoryPath();
        final MultiRegionS3Client client = origPath.getFileSystem().getClient();

        if (!dirIsEmpty(dirPath)) {
            throw new DirectoryNotEmptyException(origPath.toString());
        }

        try {
            client.deleteObject(dirPath.getBucketName(), dirPath.getBlobName());
        } catch (SdkException e) {
            throw AwsUtils.toIOE(e, origPath);
        }
    }

    static BaseFileAttributes createBucketFileAttributes(final Bucket bucket, final S3Path bucketPath) {
        Instant bucketCreationTime = bucket.creationDate();
        if (bucketCreationTime == null) {
            bucketCreationTime = Instant.ofEpochMilli(0);
        }

        final FileTime time = FileTime.from(bucketCreationTime);

        return new BaseFileAttributes(false,//
            bucketPath,//
            time,//
            time,//
            time,//
            0L,//
            false,//
            false,//
            null);
    }
}
