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
 *   Jun 29, 2020 (bjoern): created
 */
package org.knime.cloud.aws.filehandling.s3.fs;

import java.io.IOException;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.knime.filehandling.core.connections.base.BasePathIterator;
import org.knime.filehandling.core.connections.base.PagedPathIterator;
import org.knime.filehandling.core.connections.base.attributes.BaseFileAttributes;

import com.amazonaws.AbortedException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * Factory class for path iterators on S3.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public abstract class S3PathIteratorFactory {

    /**
     * Creates a new iterator instance.
     *
     * @param path path to iterate.
     * @param filter {@link Filter} instance.
     * @return {@link S3PathIterator} instance.
     * @throws IOException
     */
    public static Iterator<S3Path> create(final S3Path path, final Filter<? super Path> filter) throws IOException {
        if (path.isRoot()) {
            return new BucketIterator(path, filter);
        } else {
            return new BlobIterator(path, filter);
        }
    }

    private static class BucketIterator extends BasePathIterator<S3Path> {

        /**
         * Creates new instance.
         *
         * @param path The path to create an iterator for.
         * @param filter Filter to apply on the returned paths.
         * @throws IOException When something went wrong while fetching a page of paths.
         */
        protected BucketIterator(final S3Path path, final Filter<? super Path> filter) throws IOException {
            super(path, filter);

            @SuppressWarnings("resource")
            final S3FileSystem fs = path.getFileSystem();

            try {
                final ArrayList<S3Path> bucketPaths = new ArrayList<>();
                for (final Bucket bucket : fs.getClient().listBuckets()) {
                    final S3Path bucketPath = fs.getPath(fs.getSeparator() + bucket.getName(), fs.getSeparator());
                    final BaseFileAttributes attributes =
                        S3FileSystemProvider.createBucketFileAttributes(bucket, bucketPath);
                    fs.addToAttributeCache(bucketPath, attributes);
                    bucketPaths.add(bucketPath);
                }
                setFirstPage(bucketPaths.iterator());
            } catch (final SdkClientException e) {
                if ((e instanceof AbortedException) || (e.getCause() instanceof AbortedException)) {
                    // ignore
                } else {
                    throw e;
                }
            }
        }
    }

    private static class BlobIterator extends PagedPathIterator<S3Path> {

        private String m_continuationToken;

        /**
         * Creates new instance.
         *
         * @param path The path to create an iterator for.
         * @param filter Filter to apply on the returned paths.
         * @throws IOException When something went wrong while fetching a page of paths.
         */
        protected BlobIterator(final S3Path path, final Filter<? super Path> filter) throws IOException {
            super(path, filter);
            m_continuationToken = null;
            setFirstPage(loadNextPage());
        }

        @Override
        protected boolean hasNextPage() {
            return m_continuationToken != null;
        }

        @SuppressWarnings("resource")
        @Override
        protected Iterator<S3Path> loadNextPage() throws IOException {

            final S3FileSystem fs = m_path.getFileSystem();

            final ListObjectsV2Request listRequest = new ListObjectsV2Request();
            listRequest.withBucketName(m_path.getBucketName()) //
                .withPrefix(m_path.getBlobName()) //
                .withDelimiter(fs.getSeparator()) //
                .withEncodingType("url") //
                .withStartAfter(m_path.getBlobName()) //
                .withContinuationToken(m_continuationToken);

            try {
                final ListObjectsV2Result m_objectsListing = fs.getClient().listObjectsV2(listRequest);
                final List<S3Path> nextPage = new ArrayList<>();

                for (final S3ObjectSummary objSummary : m_objectsListing.getObjectSummaries()) {
                    nextPage.add(getPathFromSummary(objSummary));
                }

                for (final String commonPrefix : m_objectsListing.getCommonPrefixes()) {
                    nextPage.add(getPathFromPrefix(commonPrefix));
                }

                m_continuationToken = m_objectsListing.getNextContinuationToken();
                return nextPage.iterator();
            } catch (final SdkClientException e) {
                if ((e instanceof AbortedException) || (e.getCause() instanceof AbortedException)) {
                    return Collections.emptyIterator();
                } else {
                    throw e;
                }
            }
        }

        @SuppressWarnings("resource")
        private S3Path getPathFromPrefix(final String commonPrefix) {
            final S3Path path = new S3Path(m_path.getFileSystem(), m_path.getBucketName(), commonPrefix);
            final FileTime lastModified = FileTime.fromMillis(0L);
            final BaseFileAttributes attributes = new BaseFileAttributes(false, path, lastModified, lastModified,
                lastModified, 0, false, false, new S3PosixAttributesFetcher());
            m_path.getFileSystem().addToAttributeCache(path, attributes);
            return path;
        }

        @SuppressWarnings("resource")
        private S3Path getPathFromSummary(final S3ObjectSummary nextSummary) {
            final S3Path path = new S3Path(m_path.getFileSystem(), nextSummary.getBucketName(), nextSummary.getKey());
            final FileTime lastModified = FileTime.from(nextSummary.getLastModified().toInstant());

            final BaseFileAttributes attributes = new BaseFileAttributes(!path.isDirectory(), path, lastModified,
                lastModified, lastModified, nextSummary.getSize(), false, false, new S3PosixAttributesFetcher());
            m_path.getFileSystem().addToAttributeCache(path, attributes);

            return path;
        }
    }
}
