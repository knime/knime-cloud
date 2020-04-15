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
package org.knime.cloud.aws.filehandling.testing;

import java.io.ByteArrayInputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import org.knime.cloud.aws.filehandling.connections.S3FileSystem;
import org.knime.cloud.aws.filehandling.connections.S3Path;
import org.knime.filehandling.core.connections.FSConnection;
import org.knime.filehandling.core.testing.FSTestInitializer;

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
public class S3FSTestInitializer implements FSTestInitializer {

    private final FSConnection m_s3Connection;
    private final S3FileSystem m_fileSystem;
    private final String m_bucket;
    private final AmazonS3 m_s3Client;
    private final String m_uniquePrefix;

    /**
     * Creates a initializer for s3 pointing to a special test bucket.
     *
     * @param bucket
     * @param fsConnection
     */
    public S3FSTestInitializer(final String bucket, final FSConnection fsConnection) {
        m_s3Connection = fsConnection;
        m_fileSystem = (S3FileSystem) m_s3Connection.getFileSystem();
        m_uniquePrefix = UUID.randomUUID().toString();
        m_bucket = bucket;
        m_s3Client = m_fileSystem.getClient();
    }

    @Override
    public FSConnection getFSConnection() {
        return m_s3Connection;
    }

    @Override
    public Path getRoot() {
        return m_fileSystem.getPath("/", m_bucket, m_uniquePrefix + "/");
    }

    @Override
    public Path createFile(final String... pathComponents) {
        return createFileWithContent("", pathComponents);
    }

    @Override
    public Path createFileWithContent(final String content, final String... pathComponents) {
        final Path path = makePath(pathComponents);

        // create parent directory objects if necessary
        for (int i = 1; i < path.getNameCount() - 1; i++) {
            final String dirKey = path.subpath(1, i + 1).toString();
            if (!m_s3Client.doesObjectExist(m_bucket, dirKey)) {
                m_s3Client.putObject(m_bucket, dirKey, "");
            }
        }

        // create the actual object with content
        final String key = path.subpath(1, path.getNameCount()).toString();
        m_s3Client.putObject(m_bucket, key, content);

        return path;
    }

    @Override
    public void beforeTestCase() {
        final S3Path testRoot = (S3Path)getRoot();

        final ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(0);

        m_s3Client.putObject(testRoot.getBucketName(), testRoot.getBlobName(), new ByteArrayInputStream(new byte[0]),
            metadata);
    }

    @Override
    public void afterTestCase() {
        final ObjectListing bucketObjects = m_s3Client.listObjects(m_bucket);
        final List<S3ObjectSummary> objectSummaries = bucketObjects.getObjectSummaries();

        objectSummaries //
            .stream() //
            .map(S3ObjectSummary::getKey) //
            .filter(summaryKey -> summaryKey.startsWith(m_uniquePrefix)) //
            .forEach(filteredKey -> m_s3Client.deleteObject(m_bucket, filteredKey)); //
    }

}
