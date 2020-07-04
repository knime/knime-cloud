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
 *   Jun 30, 2020 (bjoern): created
 */
package org.knime.cloud.aws.filehandling.s3.fs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;

/**
 * Tests the directory listing functionality of the S3 file system, in particular the paging.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class S3PathIteratorFactoryTest {

    private static final Filter<? super Path> ALL_FILTER = (e) -> true;

    private S3FileSystem m_fs;

    private AmazonS3 m_client;

    @Before
    public void beforeTestCase() {
        m_fs = mock(S3FileSystem.class);
        when(m_fs.getSeparator()).thenReturn(S3FileSystem.PATH_SEPARATOR);

        m_client = mock(AmazonS3.class);
        when(m_fs.getClient()).thenReturn(m_client);
    }

    @Test
    public void test_root_path_iterator() throws IOException {
        // setup fixture
        final List<Bucket> buckets = new ArrayList<>();
        buckets.add(createDummyBucket("mockbucket"));
        when(m_client.listBuckets()).thenReturn(buckets);

        final S3Path bucketPath = new S3Path(m_fs, "/mockbucket/", new String[0]);
        when(m_fs.getPath("/mockbucket", "/")).thenReturn(bucketPath);

        final S3Path rootPath = new S3Path(m_fs, "/", new String[0]);
        final Iterator<S3Path> iter = S3PathIteratorFactory.create(rootPath, ALL_FILTER);
        assertTrue(iter.hasNext());
        assertEquals(bucketPath, iter.next());
        assertFalse(iter.hasNext());
    }

    @Test
    public void test_bucket_path_iterator_with_continuation() throws IOException {
        // setup fixture
        final ListObjectsV2Result mockResult1 = new ListObjectsV2Result();
        mockResult1.setBucketName("mockbucket");
        mockResult1.setCommonPrefixes(Collections.singletonList("prefix1/"));
        mockResult1.setNextContinuationToken("mocktoken");

        final ListObjectsV2Result mockResult2 = new ListObjectsV2Result();
        mockResult2.setBucketName("mockbucket");
        mockResult2.setCommonPrefixes(Collections.singletonList("prefix2/"));

        when(m_client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(mockResult1).thenReturn(mockResult2);

        final S3Path bucketPath = new S3Path(m_fs, "/mockbucket/", new String[0]);
        final Iterator<S3Path> iter = S3PathIteratorFactory.create(bucketPath, ALL_FILTER);
        assertTrue(iter.hasNext());
        assertEquals(new S3Path(m_fs, "/mockbucket/prefix1/", new String[0]), iter.next());
        assertTrue(iter.hasNext());
        assertEquals(new S3Path(m_fs, "/mockbucket/prefix2/", new String[0]), iter.next());
        assertFalse(iter.hasNext());
    }


    private Bucket createDummyBucket(final String name) {
        return new Bucket(name);
    }
}
