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
package org.knime.cloud.aws.filehandling.connections;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessDeniedException;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileStore;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileTime;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.core.node.NodeLogger;
import org.knime.filehandling.core.connections.base.BaseFileSystem;
import org.knime.filehandling.core.connections.base.BaseFileSystemProvider;
import org.knime.filehandling.core.connections.base.attributes.FSBasicAttributes;
import org.knime.filehandling.core.connections.base.attributes.FSFileAttributeView;
import org.knime.filehandling.core.connections.base.attributes.FSFileAttributes;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.Grant;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.S3Object;

/**
 * File system provider for {@link S3FileSystem}s.
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class S3FileSystemProvider extends BaseFileSystemProvider {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(S3FileSystemProvider.class);

    /**
     *
     */
    public static final String CONNECTION_INFORMATION = "ConnectionInformation";

    private final ClientConfiguration m_clientConfig;

    private final URI m_uri;

    private final long m_cacheTimeToLive;

    private final boolean m_normalizePaths;

    /**
     * Constructs a file system provider for {@link S3FileSystem}s.
     *
     * @param clientConfig the {@link ClientConfiguration} to use
     * @param uri
     * @param cacheTimeToLive the timeToLive for the attributes cache.
     * @param normalizePaths
     */
    public S3FileSystemProvider(final ClientConfiguration clientConfig, final URI uri, final long cacheTimeToLive,
        final boolean normalizePaths) {
        Objects.requireNonNull(clientConfig);
        m_clientConfig = clientConfig;
        m_uri = uri;
        m_cacheTimeToLive = cacheTimeToLive;
        m_normalizePaths = normalizePaths;
    }

    /**
     * @return the {@link ClientConfiguration}
     */
    public ClientConfiguration getClientConfig() {
        return m_clientConfig;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getScheme() {
        return "s3";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Path getPath(final URI uri) {
        return new S3Path((S3FileSystem)getFileSystem(m_uri), uri.getPath());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SeekableByteChannel newByteChannel(final Path path, final Set<? extends OpenOption> options,
        final FileAttribute<?>... attrs) throws IOException {
        return new S3SeekableByteChannel(toS3Path(path), options);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void createDirectory(final Path dir, final FileAttribute<?>... attrs) throws IOException {
        final S3Path s3Path = toS3Path(dir);

        if (exists(s3Path)) {
            throw new FileAlreadyExistsException(String.format("Already exists: %s", s3Path));
        }

        final String bucketName = s3Path.getBucketName();

        if (s3Path.getBlobName() != null) {
            // create empty object
            final ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(0);
            final String objectKey =
                s3Path.getBlobName().endsWith("/") ? s3Path.getBlobName() : s3Path.getBlobName() + "/";
            s3Path.getFileSystem().getClient().putObject(bucketName, objectKey, new ByteArrayInputStream(new byte[0]),
                metadata);
        } else {
            s3Path.getFileSystem().getClient().createBucket(bucketName);
        }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copy(final Path source, final Path target, final CopyOption... options) throws IOException {
        final S3Path sourceS3Path = toS3Path(source);
        final S3Path targetS3Path = (S3Path)target;
        final AmazonS3 client = sourceS3Path.getFileSystem().getClient();
        try {
            if (sourceS3Path.getBlobName() != null) {
                client.copyObject(sourceS3Path.getBucketName(), sourceS3Path.getBlobName(),
                    targetS3Path.getBucketName(), targetS3Path.getBlobName());
            } else {
                createDirectory(targetS3Path);
            }
        } catch (final Exception ex) {
            throw new IOException(ex);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void move(final Path source, final Path target, final CopyOption... options) throws IOException {
        final S3Path sourceS3Path = toS3Path(source);
        final S3Path targetS3Path = (S3Path)target;
        if (sourceS3Path.isDirectory()) {
            moveDir(sourceS3Path, targetS3Path);
            return;
        }
        final AmazonS3 client = sourceS3Path.getFileSystem().getClient();
        try {
            client.copyObject(sourceS3Path.getBucketName(), sourceS3Path.getBlobName(), targetS3Path.getBucketName(),
                targetS3Path.getBlobName());
        } catch (final Exception ex) {
            throw new IOException(ex);
        }

        delete(sourceS3Path);
    }

    private static void moveDir(final S3Path sourceS3Path, final S3Path targetS3Path) {
        // We have to move every blob with this prefix
        final AmazonS3 client = sourceS3Path.getFileSystem().getClient();

        ListObjectsV2Request listRequest = new ListObjectsV2Request();
        listRequest.withBucketName(sourceS3Path.getBucketName()).withPrefix(sourceS3Path.getBlobName())
            .withDelimiter(sourceS3Path.getFileSystem().getSeparator()).withEncodingType("url")
            .withStartAfter(sourceS3Path.getBlobName());

        final ListObjectsV2Result list = client.listObjectsV2(listRequest);
        list.getObjectSummaries().forEach(p -> {
            client.copyObject(p.getBucketName(), p.getKey(), targetS3Path.getBucketName(),
                p.getKey().replace(sourceS3Path.getBlobName(), targetS3Path.getBlobName() + "/"));

            client.deleteObject(p.getBucketName(), p.getKey());
            sourceS3Path.getFileSystem()
                .removeFromAttributeCache(p.getBucketName() + sourceS3Path.getFileSystem().getSeparator() + p.getKey());
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isSameFile(final Path path, final Path path2) throws IOException {
        return path.equals(path2);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isHidden(final Path path) throws IOException {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FileStore getFileStore(final Path path) throws IOException {
        return path.getFileSystem().getFileStores().iterator().next();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void checkAccess(final Path path, final AccessMode... modes) throws IOException {
        final S3Path s3Path = toS3Path(path);
        if (s3Path.isVirtualRoot()) {
            return;
        }
        if (!exists(s3Path)) {
            throw new AccessDeniedException(s3Path.toString());
        }
        if (s3Path.isDirectory()) {
            return;
        }

        AccessControlList acl;
        try {
            acl = s3Path.getFileSystem().getClient().getObjectAcl(s3Path.getBucketName(), s3Path.getBlobName());
        } catch (final AmazonServiceException ex) {
            throw new AccessDeniedException(ex.getMessage());
        }
        for (final AccessMode mode : modes) {
            switch (mode) {
                case EXECUTE:
                    throw new AccessDeniedException(s3Path.toString());
                case READ:
                    if (!containsPermission(EnumSet.of(Permission.FullControl, Permission.Read), acl)) {
                        throw new AccessDeniedException(s3Path.toString(), null, "file is not readable");
                    }
                    break;
                case WRITE:
                    if (!containsPermission(EnumSet.of(Permission.FullControl, Permission.Write), acl)) {
                        throw new AccessDeniedException(s3Path.toString(), null, "file is not readable");
                    }
                    break;
            }
        }

    }

    private static boolean containsPermission(final EnumSet<Permission> permissions, final AccessControlList acl) {
        for (final Grant grant : acl.getGrantsAsList()) {
            if (grant.getGrantee().getIdentifier().equals(acl.getOwner().getId())
                && permissions.contains(grant.getPermission())) {
                return true;
            }
        }
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public <V extends FileAttributeView> V getFileAttributeView(final Path path, final Class<V> type,
        final LinkOption... options) {

        return (V)new FSFileAttributeView(path.toString(),
            () -> (FSFileAttributes)readAttributes(path, BasicFileAttributes.class));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setAttribute(final Path path, final String attribute, final Object value, final LinkOption... options)
        throws IOException {

        throw new UnsupportedOperationException();

    }

    private static S3Path toS3Path(final Path path) {
        if (!(path instanceof S3Path)) {
            throw new IllegalArgumentException(
                String.format("Path has to be an instance of %s", S3Path.class.getName()));
        }
        return (S3Path)path.normalize();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BaseFileSystem createFileSystem(final URI uri, final Map<String, ?> env) {
        CloudConnectionInformation connInfo = null;
        if (env.containsKey(CONNECTION_INFORMATION)) {
            connInfo = (CloudConnectionInformation)env.get(CONNECTION_INFORMATION);
        }
        return new S3FileSystem(this, uri, env, connInfo, m_cacheTimeToLive, m_normalizePaths);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean exists(final Path path) {
        final S3Path s3Path = toS3Path(path);
        if (s3Path.getBucketName() == null || s3Path.getFileSystem().hasCachedAttributes(s3Path.toString())) {
            //This is the fake S3 root, or we have valid data in the cache.
            return true;
        }
        final AmazonS3 client = s3Path.getFileSystem().getClient();

        boolean exists = false;
        if (s3Path.getBlobName() != null) {
            try {
                exists = client.doesObjectExist(s3Path.getBucketName(), s3Path.getBlobName());
            } catch (final AmazonS3Exception e) {
                LOGGER.warn(e);
            }
            if (!exists && s3Path.isDirectory()) {
                final ListObjectsV2Request request = new ListObjectsV2Request();
                request.withBucketName(s3Path.getBucketName()).withPrefix(s3Path.getBlobName())
                    .withDelimiter(s3Path.getBlobName()).withDelimiter(s3Path.getFileSystem().getSeparator())
                    .withEncodingType("url").withStartAfter(s3Path.getBlobName()).setMaxKeys(1);

                ObjectListing listObjects;
                try {
                    listObjects = client.listObjects(s3Path.getBucketName(), s3Path.getBlobName());
                    exists = !listObjects.getObjectSummaries().isEmpty() || !listObjects.getCommonPrefixes().isEmpty();
                } catch (final AmazonServiceException ex) {
                    LOGGER.warn(ex);
                }
            }

            return exists;
        } else {
            try {
                return client.doesBucketExistV2(s3Path.getBucketName());
            } catch (final AmazonS3Exception e) {
                LOGGER.warn(e);
                return false;
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InputStream newInputStreamInternal(final Path path, final OpenOption... options) throws IOException {
        InputStream inputStream;
        final S3Path s3path = toS3Path(path);
        if (s3path.getBlobName() == null) {
            throw new IOException("Cannot open input stream on bucket.");
        }
        try {

            final S3Object object =
                s3path.getFileSystem().getClient().getObject(s3path.getBucketName(), s3path.getBlobName());
            inputStream = object.getObjectContent();

            if (inputStream == null) {
                object.close();
                throw new IOException(String.format("Could not read path %s", s3path));
            }

        } catch (final Exception ex) {
            throw new IOException(ex);
        }

        return inputStream;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OutputStream newOutputStreamInternal(final Path path, final OpenOption... options) throws IOException {
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
        return Channels.newOutputStream(newByteChannel(path, opts));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<Path> createPathIterator(final Path dir, final Filter<? super Path> filter) throws IOException {
        return new S3PathIterator(dir, filter);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected FSFileAttributes fetchAttributesInternal(final Path path, final Class<?> type) throws IOException {
        final S3Path pathS3 = toS3Path(path);
        if (type == BasicFileAttributes.class) {
            return new FSFileAttributes(!pathS3.isDirectory(), pathS3, p -> {

                FileTime lastmod = FileTime.fromMillis(0L);
                long size = 0;

                final S3Path s3Path = (S3Path)p;
                try {
                    if (s3Path.getBlobName() != null) {
                        final ObjectMetadata objectMetadata = s3Path.getFileSystem().getClient()
                            .getObjectMetadata(s3Path.getBucketName(), s3Path.getBlobName());

                        final Date metaDataLastMod = objectMetadata.getLastModified();

                        lastmod = metaDataLastMod != null ? FileTime.from(metaDataLastMod.toInstant())
                            : FileTime.from(getBucket(s3Path).getCreationDate().toInstant());
                        size = objectMetadata.getContentLength();
                    } else {
                        //
                        lastmod = FileTime.from(getBucket(s3Path).getCreationDate().toInstant());
                    }

                } catch (final Exception e) {
                    // If we do not have metadata we use fall back values
                }

                return new FSBasicAttributes(lastmod, lastmod, lastmod, size, false, false);
            });
        }
        throw new UnsupportedOperationException(String.format("only %s supported", BasicFileAttributes.class));
    }

    /**
     * @param s3Path the path to get the bucket for.
     * @return a {@link Bucket} if a bucket with that name exists in S3, null otherwise.
     */
    public Bucket getBucket(final S3Path s3Path) {

        for (final Bucket buck : s3Path.getFileSystem().getClient().listBuckets()) {
            if (buck.getName().equals(s3Path.getBucketName())) {
                return buck;
            }
        }
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void deleteInternal(final Path path) throws IOException {
        final S3Path s3Path = toS3Path(path);
        final AmazonS3 client = s3Path.getFileSystem().getClient();
        try {
            if (s3Path.getBlobName() != null) {
                client.deleteObject(s3Path.getBucketName(), s3Path.getBlobName());
            } else {
                client.deleteBucket(s3Path.getBucketName());
            }
        } catch (final Exception ex) {
            throw new IOException(ex);
        }
    }

}
