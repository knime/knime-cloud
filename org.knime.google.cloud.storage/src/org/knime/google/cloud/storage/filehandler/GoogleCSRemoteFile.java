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
 */
package org.knime.google.cloud.storage.filehandler;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.lang3.StringUtils;
import org.knime.base.filehandling.remote.files.ConnectionMonitor;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.cloud.core.file.CloudRemoteFile;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.util.FileUtil;
import org.knime.google.cloud.storage.util.GoogleCloudStorageConnectionInformation;

import com.google.api.client.googleapis.batch.BatchRequest;
import com.google.api.client.googleapis.batch.json.JsonBatchCallback;
import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.HttpHeaders;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.Bucket;
import com.google.api.services.storage.model.Buckets;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;

/**
 * Implementation of {@link CloudRemoteFile} for Google Cloud Storage.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class GoogleCSRemoteFile extends CloudRemoteFile<GoogleCSConnection> {

    private static final int MAX_BATCH_SIZE = 800; // max 1000

    private static final int MAX_PAGES = 1000;

    private static final NodeLogger LOGGER = NodeLogger.getLogger(GoogleCSRemoteFile.class);

    /**
     * Default constructor.
     *
     * @param uri URI of the storage object
     * @param connectionInformation connection informations to use
     * @param connectionMonitor connection monitor to use
     */
    public GoogleCSRemoteFile(final URI uri, final GoogleCloudStorageConnectionInformation connectionInformation,
        final ConnectionMonitor<GoogleCSConnection> connectionMonitor) {
        this(uri, connectionInformation, connectionMonitor, null);
    }

    /**
     * Default constructor with cache initialized from given {@link StorageObject}.
     *
     * @param uri URI of the storage object
     * @param connectionInformation connection informations to use
     * @param connectionMonitor connection monitor to use
     * @param object storage object to initialize cache
     */
    public GoogleCSRemoteFile(final URI uri, final GoogleCloudStorageConnectionInformation connectionInformation,
        final ConnectionMonitor<GoogleCSConnection> connectionMonitor, final StorageObject object) {
        super(uri, connectionInformation, connectionMonitor);
        CheckUtils.checkArgumentNotNull(connectionInformation, "Connection Information mus not be null");
        if (object != null) {
            m_exists = true;
            m_containerName = object.getBucket();
            m_blobName = object.getName();
            m_lastModified = object.getUpdated().getValue();
            m_size = object.getSize().longValue();
        }
    }

    private Storage getClient() throws Exception {
        return getOpenedConnection().getClient();
    }

    private String getProjectId() {
        return ((GoogleCloudStorageConnectionInformation)getConnectionInformation()).getProject();
    }

    @Override
    protected GoogleCSConnection createConnection() {
        return new GoogleCSConnection((GoogleCloudStorageConnectionInformation)getConnectionInformation());
    }

    @Override
    protected boolean doesContainerExist(final String bucketName) throws Exception {
        try {
            return getClient().buckets().get(bucketName).execute() != null;
        } catch (final GoogleJsonResponseException ex) {
            if (ex.getStatusCode() == 404) {
                return false;
            } else if (ex.getStatusCode() == 403) {
                throw new RemoteFile.AccessControlException(ex);
            } else {
                throw ex;
            }
        }
    }

    @Override
    protected boolean doestBlobExist(final String bucketName, final String objectName) throws Exception {
        try {
            if (objectName.endsWith(DELIMITER)) {
                final Objects objects = getClient().objects()
                    .list(bucketName)
                    .setDelimiter(DELIMITER)
                    .setPrefix(objectName)
                    .setFields("kind,items(name),prefixes")
                    .setMaxResults(1l)
                    .execute();
                return (objects.getPrefixes() != null && !objects.getPrefixes().isEmpty())
                    || (objects.getItems() != null && !objects.getItems().isEmpty());
            } else {
                return getClient().objects().get(bucketName, objectName).setFields("kind,name").execute() != null;
            }
        } catch (final GoogleJsonResponseException ex) {
            if (ex.getStatusCode() == 404) {
                return false;
            } else if (ex.getStatusCode() == 403) {
                throw new RemoteFile.AccessControlException(ex);
            } else {
                throw ex;
            }
        }
    }

    @Override
    protected GoogleCSRemoteFile[] listRootFiles() throws Exception {
        final ArrayList<GoogleCSRemoteFile> files = new ArrayList<>();
        final com.google.api.services.storage.Storage.Buckets.List req =
            getClient().buckets().list(getProjectId()).setFields("kind,items(name),nextPageToken");

        Buckets buckets = req.execute();
        for (int page = 1; page < MAX_PAGES; page++) {
            if (buckets.getItems() != null) { // files
                for (final Bucket bucket : buckets.getItems()) {
                    files.add(getRemoteFile(bucket.getName(), null, null));
                }
            }

            if (page + 1 == MAX_PAGES) {
                LOGGER.warn("Max pages count (" + MAX_PAGES + ") in bucket listing reached, ignoring other pages.");
            }

            if (!StringUtils.isBlank(buckets.getNextPageToken())) {
                buckets = req.setPageToken(buckets.getNextPageToken()).execute();
            } else {
                break;
            }
        }

        return files.toArray(new GoogleCSRemoteFile[0]);
    }

    private GoogleCSRemoteFile getRemoteFile(final String bucketName, final String objectName, final StorageObject obj)
        throws URISyntaxException {
        final String path = createContainerPath(bucketName) + (objectName != null ? objectName : "");
        final URI uri = new URI(getURI().getScheme(), getURI().getUserInfo(), getURI().getHost(), -1, path, null, null);
        return new GoogleCSRemoteFile(uri, (GoogleCloudStorageConnectionInformation)getConnectionInformation(),
            getConnectionMonitor(), obj);
    }

    @Override
    protected GoogleCSRemoteFile[] listDirectoryFiles() throws Exception {
        final String bucketName = getContainerName();
        final ArrayList<GoogleCSRemoteFile> files = new ArrayList<>();
        final String prefix = StringUtils.isBlank(getBlobName()) ? "" : getBlobName();
        final com.google.api.services.storage.Storage.Objects.List req = getClient().objects().list(bucketName)
            .setDelimiter(DELIMITER)
            .setPrefix(prefix)
            .setFields("kind,items(bucket,name,size,updated),prefixes,nextPageToken");

        Objects objects = req.execute();
        for (int page = 1; page < MAX_PAGES; page++) {
            if (objects.getPrefixes() != null) { // directories
                for (final String name : objects.getPrefixes()) {
                    if (!name.equals(prefix)) {
                        files.add(getRemoteFile(bucketName, name, null));
                    }
                }
            }

            if (objects.getItems() != null) { // files
                for (final StorageObject obj : objects.getItems()) {
                    if (!obj.getName().equals(prefix)) {
                        files.add(getRemoteFile(bucketName, obj.getName(), obj));
                    }
                }
            }

            if (page + 1 == MAX_PAGES) {
                LOGGER.warn(
                    "Max pages count (" + MAX_PAGES + ") in directory files listing reached, ignoring other pages.");
            }

            if (!StringUtils.isBlank(objects.getNextPageToken())) {
                objects = req.setPageToken(objects.getNextPageToken()).execute();
            } else {
                break;
            }
        }

        return files.toArray(new GoogleCSRemoteFile[0]);
    }

    /**
     * Fetch meta data and initialize cache.
     */
    private void fetchMetaData() throws Exception {
        try {
            final StorageObject obj = getClient().objects().get(getContainerName(), getBlobName())
                .setFields("kind,items(bucket,name,size,updated)").execute();
            m_size = obj.getSize().longValue();
            m_lastModified = obj.getUpdated().getValue();
            m_exists = true;

        } catch (final GoogleJsonResponseException ex) {
            m_size = m_lastModified = null;
            if (ex.getStatusCode() == 404) {
                m_exists = false;
            } else if (ex.getStatusCode() == 403) {
                m_exists = null;
                throw new RemoteFile.AccessControlException(ex);
            } else {
                m_exists = null;
                throw ex;
            }
        }
    }

    @Override
    protected long getBlobSize() throws Exception {
        if (m_size == null) {
            fetchMetaData();
        }
        return m_size;
    }

    @Override
    protected long getLastModified() throws Exception {
        if (m_lastModified == null) {
            fetchMetaData();
        }
        return m_lastModified;
    }

    @Override
    protected boolean deleteContainer() throws Exception {
        getClient().buckets().delete(getContainerName()).execute();
        return true;
    }

    @Override
    protected boolean deleteDirectory() throws Exception {
        final String bucket = getContainerName();
        final BatchRequest batch = getClient().batch();
        final JsonBatchCallback<Void> batchHandler = createDeleteBatchHandler();
        final LinkedBlockingQueue<GoogleCSRemoteFile> queue = new LinkedBlockingQueue<>();
        queue.add(this);
        while (!queue.isEmpty()) {
            final GoogleCSRemoteFile file = queue.poll();
            if (file.isDirectory()) {
                queue.addAll(Arrays.asList(file.listDirectoryFiles()));
            }

            getClient().objects().delete(bucket, file.getBlobName()).queue(batch, batchHandler);
            if (batch.size() >= MAX_BATCH_SIZE) {
                batch.execute();
            }
        }

        if (batch.size() > 0) {
            batch.execute();
        }

        return true;
    }

    private static JsonBatchCallback<Void> createDeleteBatchHandler() {
        return new JsonBatchCallback<Void>() {
            @Override
            public void onSuccess(final Void t, final HttpHeaders responseHeaders) throws IOException {
                // nothing to do
            }

            @Override
            public void onFailure(final GoogleJsonError e, final HttpHeaders responseHeaders) throws IOException {
                LOGGER.warn("Failure in batch delete: " + e.getMessage());
            }
        };
    }

    @Override
    protected boolean deleteBlob() throws Exception {
        getClient().objects().delete(getContainerName(), getBlobName()).execute();
        return true;
    }

    @Override
    protected boolean createContainer() throws Exception {
        final Bucket bucket = new Bucket();
        bucket.setName(getContainerName());
        getClient().buckets().insert(getProjectId(), bucket).execute();
        return true;
    }

    @Override
    protected boolean createDirectory(final String dirName) throws Exception {
        final ByteArrayContent emptyContent = new ByteArrayContent("", new byte[0]);
        getClient().objects().insert(getContainerName(), null, emptyContent).setName(getBlobName()).execute();
        return true;
    }

    @Override
    public InputStream openInputStream() throws Exception {
        return getClient().objects().get(getContainerName(), getBlobName()).executeMediaAsInputStream();
    }

    /**
     * Upload a object using a temporary file.
     */
    @Override
    public OutputStream openOutputStream() throws Exception {
        final File tmpFile = FileUtil.createTempFile("gcs-" + getName(), "");
        resetCache();
        return new UploadOutputStream(getClient(), getContainerName(), getBlobName(), tmpFile);
    }

    /**
     * Generate a signed public URL with expiration time.
     *
     * @param expirationSeconds URL expiration time in seconds from now
     * @return signed URL
     * @throws Exception
     */
    public String getSignedURL(final long expirationSeconds) throws Exception {
        return getConnection().getSigningURL(expirationSeconds, getContainerName(), getBlobName());
    }

    /**
     * @return Spark compatible cluster URL (gs://<containername>/<path>)
     */
    @Override
    public URI getHadoopFilesystemURI() throws Exception {
        final String scheme = "gs";
        final String container = getContainerName();
        final String blobName = DELIMITER + Optional.ofNullable(getBlobName()).orElseGet(() -> "");
        return new URI(scheme, null, container, -1, blobName, null, null);
    }
}
