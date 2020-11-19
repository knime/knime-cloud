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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.Files;
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

import org.knime.core.node.NodeLogger;
import org.knime.filehandling.core.connections.base.BaseFileSystemProvider;
import org.knime.filehandling.core.connections.base.attributes.BaseFileAttributes;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

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

        if (dir.getBlobName() != null) {
            final String objectKey = dir.toDirectoryPath().getBlobName();
            PutObjectRequest req = PutObjectRequest.builder()//
                .bucket(bucketName)//
                .key(objectKey)//
                .applyMutation(dir.getFileSystem()::populateSseParams)//
                .build();
            dir.getFileSystem().getClient().putObject(req, RequestBody.empty());
        } else {
            dir.getFileSystem().getClient().createBucket(b -> b.bucket(bucketName));
        }
    }

    @SuppressWarnings("resource")
    @Override
    protected void copyInternal(final S3Path source, final S3Path target, final CopyOption... options) throws IOException {
        final S3Client client = source.getFileSystem().getClient();

        if (!source.isDirectory()) {
            try {
                CopyObjectRequest req = CopyObjectRequest.builder()//
                    .copySource(encodeCopySource(source.getBucketName(), source.getBlobName()))
                    .destinationBucket(target.getBucketName())//
                    .destinationKey(target.getBlobName())//
                    .applyMutation(target.getFileSystem()::populateSseParams)//
                    .build();
                client.copyObject(req);
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

    private static String encodeCopySource(final String bucket, final String blob) throws UnsupportedEncodingException {
        return URLEncoder.encode(bucket + "/" + blob, StandardCharsets.UTF_8.toString());
    }

    @SuppressWarnings("resource")
    private static boolean dirIsEmpty(final S3Path dir) {
        final S3Client client = dir.getFileSystem().getClient();

        final ListObjectsV2Request listRequest = ListObjectsV2Request.builder()//
            .bucket(dir.getBucketName())//
            .prefix(dir.getBlobName())//
            .delimiter(dir.getFileSystem().getSeparator())//
            .encodingType("url")//
            .startAfter(dir.getBlobName())//
            .maxKeys(1)//
            .build();

        return client.listObjectsV2(listRequest).keyCount() == 0;
    }

    @SuppressWarnings("resource")
    @Override
    protected void moveInternal(final S3Path source, final S3Path target, final CopyOption... options) throws IOException {
        if (Files.isDirectory(source)) {
            moveDir(source, target);
            return;
        }

        final S3Client client = source.getFileSystem().getClient();
        try {
            CopyObjectRequest req = CopyObjectRequest.builder()//
                .copySource(encodeCopySource(source.getBucketName(), source.getBlobName()))
                .destinationBucket(target.getBucketName())//
                .destinationKey(target.getBlobName())//
                .applyMutation(target.getFileSystem()::populateSseParams).build();
            client.copyObject(req);

            delete(source);
        } catch (final Exception ex) {
            throw new IOException(ex);
        }
    }

    @SuppressWarnings("resource")
    private static void moveDir(final S3Path sourceS3Path, final S3Path targetS3Path) throws IOException {
        S3Path sourceDirPath = sourceS3Path.toDirectoryPath();
        S3Path targetDirPath = targetS3Path.toDirectoryPath();

        if (!dirIsEmpty(targetDirPath)) {
            throw new DirectoryNotEmptyException(targetDirPath.toString());
        }

        // We have to move every blob with this prefix
        final S3Client client = sourceDirPath.getFileSystem().getClient();

        final ListObjectsV2Request listRequest = ListObjectsV2Request.builder()//
            .bucket(sourceDirPath.getBucketName())//
            .prefix(sourceDirPath.getBlobName())//
            .encodingType("url")//
            .build();

        final ListObjectsV2Response list = client.listObjectsV2(listRequest);
        for (S3Object object : list.contents()) {
            CopyObjectRequest req = CopyObjectRequest.builder()//
                .copySource(encodeCopySource(sourceDirPath.getBucketName(), object.key()))
                .destinationBucket(targetDirPath.getBucketName())//
                .destinationKey(object.key().replace(sourceDirPath.getBlobName(), targetDirPath.getBlobName()))//
                .applyMutation(targetDirPath.getFileSystem()::populateSseParams)//
                .build();
            client.copyObject(req);

            client.deleteObject(b -> b.bucket(sourceDirPath.getBucketName()).key(object.key()));

            sourceDirPath.getFileSystem().removeFromAttributeCache(
                new S3Path(sourceDirPath.getFileSystem(), sourceDirPath.getBucketName(), object.key()));
        }
    }

    @Override
    protected void checkAccessInternal(final S3Path s3Path, final AccessMode... modes) throws IOException {
        // do nothing, too complex
    }

    private static boolean doesObjectExist(final S3Client client, final String bucket, final String blob) {
        try {
            client.headObject(b -> b.bucket(bucket).key(blob));
            return true;
        } catch (NoSuchKeyException e) {//NOSONAR exceptions is expected behavior
            return false;
        }
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("resource")
    @Override
    protected InputStream newInputStreamInternal(final S3Path path, final OpenOption... options) throws IOException {

        InputStream inputStream;

        if (path.getBlobName() == null) {
            throw new IOException("Cannot open input stream on bucket.");
        }

        try {

            inputStream =
                path.getFileSystem().getClient().getObject(b -> b.bucket(path.getBucketName()).key(path.getBlobName()));

            if (inputStream == null) {
                throw new IOException(String.format("Could not read path %s", path));
            }

        } catch (NoSuchKeyException ex) {
            final NoSuchFileException noSuchFileEx = new NoSuchFileException(path.toString());
            noSuchFileEx.initCause(ex);
            throw noSuchFileEx;
        } catch (final AwsServiceException ex) {
                throw new IOException(ex);
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
        return Channels.newOutputStream(newByteChannel(path, opts));
    }

    @Override
    protected Iterator<S3Path> createPathIterator(final S3Path dir, final Filter<? super Path> filter) throws IOException {
        return S3PathIteratorFactory.create(dir.toDirectoryPath(), filter);
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
                null);
        }
    }

    private BaseFileAttributes fetchAttributesForBucket(final S3Path path) throws IOException {
        final Bucket bucket = getBucket(path);

        if (bucket == null) {
            throw new NoSuchFileException(path.toString());
        }

        return createBucketFileAttributes(bucket, path);
    }

    @SuppressWarnings("resource")
    private BaseFileAttributes fetchAttributesForObject(final S3Path path) throws NoSuchFileException {
        final S3Client client = path.getFileSystem().getClient();

        // first we try whether there is an object for the given path
        // (whether path.isDirectory() or not)
        try {
            HeadObjectResponse metadata =
                client.headObject(b -> b.bucket(path.getBucketName()).key(path.getBlobName()));
            return convertMetaDataToFileAttributes(path, metadata);
        } catch (NoSuchKeyException e) {//NOSONAR exceptions is expected behavior
            //object does not exist
        }

        final S3Path dirPath = path.toDirectoryPath();

        // directory case (1): when given "/path", but it does not exist, then we check for /path/
        if (!path.isDirectory()) {
            try {
                HeadObjectResponse metadata =
                    client.headObject(b -> b.bucket(dirPath.getBucketName()).key(dirPath.getBlobName()));
                return convertMetaDataToFileAttributes(dirPath, metadata);
            } catch (NoSuchKeyException e) { //NOSONAR exceptions is expected behavior
                //object does not exist
            }
        }

        // directory case (2): last possibility, when neither "/path" nor "/path/" exist, it could be
        // that the "directory" is just the common prefix of some objects.
        final ListObjectsV2Request request = ListObjectsV2Request.builder()//
                .bucket(dirPath.getBucketName())//
                .prefix(dirPath.getBlobName())//
                .startAfter(dirPath.getBlobName())//
                .maxKeys(1)//
                .build();

        final ListObjectsV2Response result = client.listObjectsV2(request);
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
        }

        throw new NoSuchFileException(path.toString());
    }

    private BaseFileAttributes convertMetaDataToFileAttributes(final S3Path path,
        final HeadObjectResponse objectMetadata) throws NoSuchFileException {
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
     * @throws NoSuchFileException
     */
    private FileTime determineObjectLastModificationTime(final S3Path path, final HeadObjectResponse objectMetadata)
        throws NoSuchFileException {
        final FileTime lastMod;
        if (objectMetadata.lastModified() != null) {
            lastMod = FileTime.from(objectMetadata.lastModified());
        } else {
            final Bucket bucket = getBucket(path);

            // bucket was deleted in the meantime
            if (bucket == null) {
                throw new NoSuchFileException(path.toString());
            }

            if (bucket.creationDate() != null) {
                lastMod = FileTime.from(bucket.creationDate());
            } else {
                lastMod = FileTime.fromMillis(0);
            }
        }
        return lastMod;
    }

    @SuppressWarnings("resource")
    Bucket getBucket(final S3Path path) {
        for (final Bucket buck : path.getFileSystem().getClient().listBuckets().buckets()) {
            if (buck.name().equals(path.getBucketName())) {
                return buck;
            }
        }
        return null;
    }

    @SuppressWarnings("resource")
    @Override
    protected void deleteInternal(final S3Path path) throws IOException {
        try {
            final S3Client client = path.getFileSystem().getClient();

            if (path.getBlobName() != null) {
                if (!path.isDirectory() && doesObjectExist(client, path.getBucketName(), path.getBlobName())) {
                    // regular file. deleteObject does not fail if the object has been deleted in the meantime
                    client.deleteObject(b -> b.bucket(path.getBucketName()).key(path.getBlobName()));
                } else {
                    deleteFolder(path);
                }
            } else {
                client.deleteBucket(b -> b.bucket(path.getBucketName()));
            }
        } catch (final RuntimeException ex) {
            throw new IOException(ex);
        }
    }

    @SuppressWarnings("resource")
    private void deleteFolder(final S3Path origPath) throws DirectoryNotEmptyException {
        final S3Path dirPath = origPath.toDirectoryPath();
        final S3Client client = getFileSystemInternal().getClient();

        if (!dirIsEmpty(dirPath)) {
            throw new DirectoryNotEmptyException(origPath.toString());
        }

        client.deleteObject(b -> b.bucket(dirPath.getBucketName()).key(dirPath.getBlobName()));
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
