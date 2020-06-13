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
 *   Jan 7, 2020 (Tobias Urhaug, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.cloud.aws.filehandling.s3.testing;

import java.io.ByteArrayInputStream;
import java.util.List;

import org.knime.cloud.aws.filehandling.s3.fs.S3FileSystem;
import org.knime.cloud.aws.filehandling.s3.fs.S3Path;
import org.knime.filehandling.core.connections.FSConnection;
import org.knime.filehandling.core.connections.base.BlobStorePath;
import org.knime.filehandling.core.testing.DefaultFSTestInitializer;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * S3 initializer.
 *
 * The unique prefix ensures that each instance of this test initializer has its own name space.
 *
 * @author Tobias Urhaug, KNIME GmbH, Berlin, Germany
 */
public class S3FSTestInitializer extends DefaultFSTestInitializer<S3Path, S3FileSystem> {

    private final AmazonS3 m_s3Client;

    /**
     * Creates a initializer for s3 pointing to a special test bucket.
     *
     * @param fsConnection
     */
    public S3FSTestInitializer(final FSConnection fsConnection) {
        super(fsConnection);
        m_s3Client = getFileSystem().getClient();
    }

    @Override
    public S3Path createFile(final String... pathComponents) {
        return createFileWithContent("", pathComponents);
    }

    @Override
    public S3Path createFileWithContent(final String content, final String... pathComponents) {
        final S3Path path = makePath(pathComponents);

        // create parent directory objects if necessary
        for (int i = 1; i < path.getNameCount() - 1; i++) {
            final String dirKey = path.subpath(1, i + 1).toString();
            if (!m_s3Client.doesObjectExist(path.getBucketName(), dirKey)) {
                m_s3Client.putObject(path.getBucketName(), dirKey, "");
            }
        }

        // create the actual object with content
        final String key = path.subpath(1, path.getNameCount()).toString();
        m_s3Client.putObject(path.getBucketName(), key, content);

        return path;
    }

    @Override
    protected void beforeTestCaseInternal() {
        final BlobStorePath scratchDir = getTestCaseScratchDir().toDirectoryPath();

        final ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(0);

        if (!m_s3Client.doesObjectExist(scratchDir.getBucketName(), scratchDir.getBlobName())) {
            m_s3Client.putObject(scratchDir.getBucketName(), scratchDir.getBlobName(),
                new ByteArrayInputStream(new byte[0]), metadata);
        }
    }

    @Override
    protected void afterTestCaseInternal() {
        final BlobStorePath scratchDir = getTestCaseScratchDir();

        ObjectListing bucketObjects = null;

        do {
            if (bucketObjects == null) {
                bucketObjects = m_s3Client.listObjects(scratchDir.getBucketName(), scratchDir.getBlobName());
            } else {
                bucketObjects = m_s3Client.listNextBatchOfObjects(bucketObjects);
            }

            final List<S3ObjectSummary> objectSummaries = bucketObjects.getObjectSummaries();

            objectSummaries //
                .stream() //
                .map(S3ObjectSummary::getKey) //
                .forEach(filteredKey -> m_s3Client.deleteObject(scratchDir.getBucketName(), filteredKey)); //
        } while (bucketObjects.isTruncated());
    }
}
