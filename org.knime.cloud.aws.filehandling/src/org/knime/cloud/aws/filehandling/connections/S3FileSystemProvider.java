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
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileStore;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
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
import org.knime.filehandling.core.connections.base.BaseFileSystemProvider;
import org.knime.filehandling.core.connections.base.attributes.BaseFileAttributes;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.Grant;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * File system provider for {@link S3FileSystem}s.
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class S3FileSystemProvider extends BaseFileSystemProvider<S3Path, S3FileSystem> {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(S3FileSystemProvider.class);

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
    ClientConfiguration getClientConfig() {
        return m_clientConfig;
    }

    private String getSeparator() {
        return getFileSystemInternal().getSeparator();
    }

    @Override
    public String getScheme() {
        return "s3";
    }

    @Override
    protected SeekableByteChannel newByteChannelInternal(final S3Path path, final Set<? extends OpenOption> options,
        final FileAttribute<?>... attrs) throws IOException {
        return new S3SeekableByteChannel(toS3Path(path), options);
    }

    @Override
    public void createDirectory(final Path dir, final FileAttribute<?>... attrs) throws IOException {
        final S3Path s3Path = toS3Path(dir);

        if (existsCached(s3Path)) {
            throw new FileAlreadyExistsException(s3Path.toString());
        }

        final String bucketName = s3Path.getBucketName();

        if (s3Path.getBlobName() != null) {
            // create empty object
            final ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(0);
            final String objectKey =
                s3Path.getBlobName().endsWith(getSeparator()) ? s3Path.getBlobName() : s3Path.getBlobName() + getSeparator();
            s3Path.getFileSystem().getClient().putObject(bucketName, objectKey, new ByteArrayInputStream(new byte[0]),
                metadata);
        } else {
            s3Path.getFileSystem().getClient().createBucket(bucketName);
        }

    }

    @Override
    protected void copyInternal(final S3Path source, final S3Path target, final CopyOption... options) throws IOException {
        final AmazonS3 client = source.getFileSystem().getClient();

        if (!source.isDirectory()) {
            try {
                client.copyObject(source.getBucketName(), source.getBlobName(),
                    target.getBucketName(), target.getBlobName());
            } catch (final Exception ex) {
                throw new IOException(ex);
            }
        } else {
            if (!dirIsEmpty(target)) {
                throw new DirectoryNotEmptyException(
                    String.format("Target directory %s exists and is not empty", target.toString()));
            }
            createDirectory(target);
        }
    }

    private static boolean dirIsEmpty(final S3Path dir) {
        final AmazonS3 client = dir.getFileSystem().getClient();

        final ListObjectsV2Request listRequest = new ListObjectsV2Request();
        listRequest.withBucketName(dir.getBucketName())//
            .withPrefix(dir.getBlobName())//
            .withDelimiter(dir.getFileSystem().getSeparator())//
            .withEncodingType("url")
            .withStartAfter(dir.getBlobName())
            .withMaxKeys(1);

        return client.listObjectsV2(listRequest).getKeyCount() == 0;
    }

    @Override
    protected void moveInternal(final S3Path source, final S3Path target, final CopyOption... options) throws IOException {
        if (source.isDirectory()) {
            moveDir(source, target);
            return;
        }

        final AmazonS3 client = source.getFileSystem().getClient();
        try {
            client.copyObject(source.getBucketName(), source.getBlobName(), target.getBucketName(),
                target.getBlobName());
        } catch (final Exception ex) {
            throw new IOException(ex);
        }

        delete(source);
    }

    private void moveDir(final S3Path sourceS3Path, final S3Path targetS3Path)
        throws DirectoryNotEmptyException {

        if (!dirIsEmpty(targetS3Path)) {
            throw new DirectoryNotEmptyException(targetS3Path.toString());
        }

        // We have to move every blob with this prefix
        final AmazonS3 client = sourceS3Path.getFileSystem().getClient();

        final ListObjectsV2Request listRequest = new ListObjectsV2Request();
        listRequest.withBucketName(sourceS3Path.getBucketName()).withPrefix(sourceS3Path.getBlobName())
            .withDelimiter(sourceS3Path.getFileSystem().getSeparator()).withEncodingType("url")
            .withStartAfter(sourceS3Path.getBlobName());

        final ListObjectsV2Result list = client.listObjectsV2(listRequest);
        list.getObjectSummaries().forEach(p -> {
            client.copyObject(p.getBucketName(), p.getKey(), targetS3Path.getBucketName(),
                p.getKey().replace(sourceS3Path.getBlobName(), targetS3Path.getBlobName() + getSeparator()));

            client.deleteObject(p.getBucketName(), p.getKey());
            sourceS3Path.getFileSystem()
                .removeFromAttributeCache(new S3Path(sourceS3Path.getFileSystem(), p.getBucketName(), p.getKey()));
        });
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

    @Override
    protected void checkAccessInternal(final S3Path s3Path, final AccessMode... modes) throws IOException {
        if (s3Path.isVirtualRoot() || s3Path.isDirectory()) {
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

    private S3Path toS3Path(final Path path) {
        checkPathProvider(path);
        return (S3Path)path.normalize();
    }

    @Override
    public S3FileSystem createFileSystem(final URI uri, final Map<String, ?> env) {
        CloudConnectionInformation connInfo = null;
        if (env.containsKey(CONNECTION_INFORMATION)) {
            connInfo = (CloudConnectionInformation)env.get(CONNECTION_INFORMATION);
        }
        return new S3FileSystem(this, uri, env, connInfo, m_cacheTimeToLive, m_normalizePaths);
    }

    @Override
    protected boolean exists(final S3Path path) {
        if (path.getBucketName() == null) {
            //This is the fake S3 root, or we have valid data in the cache.
            return true;
        }
        final AmazonS3 client = path.getFileSystem().getClient();

        boolean exists = false;
        if (path.getBlobName() != null) {
            try {
                exists = client.doesObjectExist(path.getBucketName(), path.getBlobName());
            } catch (final AmazonS3Exception e) {
                LOGGER.warn(e);
            }

            // when given /path also check for existence of /path/
            if (!exists && !path.isDirectory()) {
                try {
                    exists = client.doesObjectExist(path.getBucketName(), path.getBlobName() + getSeparator());
                } catch (final AmazonS3Exception e) {
                    LOGGER.warn(e);
                }
            }

            // when given /path or /path/ also check for /path/bla
            if (!exists) {
                final String blobFolderName = path.getBlobName().endsWith(getSeparator())//
                    ? path.getBlobName()//
                    : path.getBlobName() + getSeparator();

                final ListObjectsV2Request request = new ListObjectsV2Request();
                request.withBucketName(path.getBucketName())//
                    .withPrefix(blobFolderName)//
                    .withStartAfter(blobFolderName)
                    .setMaxKeys(1);

                try {
                    final ListObjectsV2Result result = client.listObjectsV2(request);
                    exists = !result.getObjectSummaries().isEmpty() || !result.getCommonPrefixes().isEmpty();
                } catch (final AmazonServiceException ex) {
                    LOGGER.warn(ex);
                }
            }

            return exists;
        } else {
            try {
                return client.doesBucketExistV2(path.getBucketName());
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
    protected InputStream newInputStreamInternal(final S3Path path, final OpenOption... options) throws IOException {

        InputStream inputStream;

        if (path.getBlobName() == null) {
            throw new IOException("Cannot open input stream on bucket.");
        }

        try {

            final S3Object object =
                path.getFileSystem().getClient().getObject(path.getBucketName(), path.getBlobName());
            inputStream = object.getObjectContent();

            if (inputStream == null) {
                object.close();
                throw new IOException(String.format("Could not read path %s", path));
            }

        } catch (final AmazonServiceException ex) {
            if (Objects.equals(ex.getErrorCode(), "NoSuchKey")) {
                final NoSuchFileException noSuchFileEx = new NoSuchFileException(path.toString());
                noSuchFileEx.initCause(ex);
                throw noSuchFileEx;
            } else {
                throw new IOException(ex);
            }
        } catch (final Exception ex) {
            throw wrapAsIOExceptionIfNecessary(ex);
        }

        return inputStream;
    }

    private static IOException wrapAsIOExceptionIfNecessary(final Exception ex) {
        IOException toReturn;

        if (ex instanceof IOException) {
            toReturn = (IOException)ex;
        } else {
            toReturn = new IOException(ex);
        }

        return toReturn;
    }

    /**
     * {@inheritDoc}
     */
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
        return Channels.newOutputStream(newByteChannel(path, opts));
    }

    @Override
    protected Iterator<S3Path> createPathIterator(final S3Path dir, final Filter<? super Path> filter) throws IOException {
        return new S3PathIterator(ensureDirPath(dir), filter);
    }

    @Override
    protected BaseFileAttributes fetchAttributesInternal(final S3Path path, final Class<?> type) throws IOException {
        if (path.getBlobName() != null) {
            return fetchAttributesForObject(path);
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
                new S3PosixAttributesFetcher());
        }
    }

    private BaseFileAttributes fetchAttributesForBucket(final S3Path path) throws IOException {
        final Bucket bucket = getBucket(path);

        if (bucket == null) {
            throw new NoSuchFileException(path.toString());
        }

        return createBucketFileAttributes(bucket, path);
    }

    private BaseFileAttributes fetchAttributesForObject(final S3Path path) throws NoSuchFileException {
        final AmazonS3 client = path.getFileSystem().getClient();

        // first we try whether there is an object for the given path
        // (whether path.isDirectory() or not)
        if (client.doesObjectExist(path.getBucketName(), path.getBlobName())) {
            return convertMetaDataToFileAttributes(path,
                client.getObjectMetadata(path.getBucketName(), path.getBlobName()));
        }

        final S3Path dirPath = ensureDirPath(path);

        // directory case (1): when given "/path", but it does not exist, then we check for /path/
        if (!path.isDirectory() && client.doesObjectExist(dirPath.getBucketName(), dirPath.getBlobName())) {
            return convertMetaDataToFileAttributes(dirPath,
                client.getObjectMetadata(dirPath.getBucketName(), dirPath.getBlobName()));
        }

        // directory case (2): last possibility, when neither "/path" nor "/path/" exist, it could be
        // that the "directory" is just the common prefix of some objects. In this case we use the
        // metadata of the first object.

        final ListObjectsV2Request request = new ListObjectsV2Request();
        request.withBucketName(path.getBucketName())//
            .withPrefix(dirPath.getBlobName())//
            .setMaxKeys(1);

        final ListObjectsV2Result result = client.listObjectsV2(request);
        if (!result.getObjectSummaries().isEmpty()) {
            final S3ObjectSummary firstObjectSummary = result.getObjectSummaries().get(0);
            final ObjectMetadata firstObjectMetaData = client.getObjectMetadata(//
                firstObjectSummary.getBucketName(),//
                firstObjectSummary.getKey());
            return convertMetaDataToFileAttributes(dirPath, firstObjectMetaData);
        }

        throw new NoSuchFileException(path.toString());
    }

    private S3Path ensureDirPath(final S3Path path) {
        if (!path.isDirectory()) {
            return getFileSystemInternal().getPath(path.toString(), getSeparator());
        } else {
            return path;
        }
    }

    private BaseFileAttributes convertMetaDataToFileAttributes(final S3Path path, final ObjectMetadata objectMetadata) throws NoSuchFileException {
        final FileTime lastMod = determineObjectLastModificationTime(path, objectMetadata);

        long size = 0;
        if (!path.isDirectory()) {
            size = objectMetadata.getContentLength();
        }

        return new BaseFileAttributes(!path.isDirectory(), path, //
            lastMod, //
            lastMod, //
            lastMod, //
            size, //
            false, //
            false, //
            new S3PosixAttributesFetcher());
    }

    /**
     * @param path
     * @param objectMetadata
     * @return
     * @throws NoSuchFileException
     */
    private FileTime determineObjectLastModificationTime(final S3Path path, final ObjectMetadata objectMetadata)
        throws NoSuchFileException {
        final FileTime lastMod;
        if (objectMetadata.getLastModified() != null) {
            lastMod = FileTime.from(objectMetadata.getLastModified().toInstant());
        } else {
            final Bucket bucket = getBucket(path);

            // bucket was deleted in the meantime
            if (bucket == null) {
                throw new NoSuchFileException(path.toString());
            }

            if (bucket.getCreationDate() != null) {
                lastMod = FileTime.from(bucket.getCreationDate().toInstant());
            } else {
                lastMod = FileTime.fromMillis(0);
            }
        }
        return lastMod;
    }

    Bucket getBucket(final S3Path path) {
        for (final Bucket buck : path.getFileSystem().getClient().listBuckets()) {
            if (buck.getName().equals(path.getBucketName())) {
                return buck;
            }
        }
        return null;
    }

    @SuppressWarnings("resource")
    @Override
    protected void deleteInternal(final S3Path path) throws IOException {
        try {
            final AmazonS3 client = path.getFileSystem().getClient();

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
        } catch (final RuntimeException ex) {
            throw new IOException(ex);
        }
    }

    @SuppressWarnings("resource")
    private void deleteFolder(final S3Path origPath) throws DirectoryNotEmptyException {
        final S3Path dirPath = ensureDirPath(origPath);
        final AmazonS3 client = getFileSystemInternal().getClient();

        if (!dirIsEmpty(dirPath)) {
            throw new DirectoryNotEmptyException(origPath.toString());
        }

        client.deleteObject(dirPath.getBucketName(), dirPath.getBlobName());
    }

    static BaseFileAttributes createBucketFileAttributes(final Bucket bucket, final S3Path bucketPath) {
        Date bucketCreationTime = bucket.getCreationDate();
        if (bucketCreationTime == null) {
            bucketCreationTime = new Date(0);
        }

        final FileTime time = FileTime.fromMillis(bucketCreationTime.getTime());

        return new BaseFileAttributes(false,//
            bucketPath,//
            time,//
            time,//
            time,//
            0L,//
            false,//
            false,//
            new S3PosixAttributesFetcher());
    }
}
