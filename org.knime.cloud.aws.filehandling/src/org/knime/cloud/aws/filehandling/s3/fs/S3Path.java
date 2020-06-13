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
 *   21.08.2019 (Mareike Hoeger, KNIME GmbH, Konstanz, Germany): created
 */

package org.knime.cloud.aws.filehandling.s3.fs;

import java.nio.file.Path;

import org.knime.filehandling.core.connections.base.BlobStorePath;

/**
 * {@link Path} implementation for {@link S3FileSystem}
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class S3Path extends BlobStorePath {

    /**
     * Creates an S3Path from the given path string
     *
     * @param fileSystem the file system
     * @param first The first name component.
     * @param more More name components.
     */
    public S3Path(final S3FileSystem fileSystem, final String first, final String[] more) {
        super(fileSystem, first, more);
    }

    /**
     * Creates an S3Path from the given bucket name and object key
     *
     * @param fileSystem the file system
     * @param bucketName the bucket name
     * @param key the object key
     */
    public S3Path(final S3FileSystem fileSystem, final String bucketName, final String key) {
        super(fileSystem, bucketName, key);
    }

    @Override
    public S3FileSystem getFileSystem() {
        return (S3FileSystem)super.getFileSystem();
    }

    @Override
    protected boolean lastComponentUsesRelativeNotation() {
        if (getFileSystem().normalizePaths()) {
            return super.lastComponentUsesRelativeNotation();
        }
        return false;
    }

    @Override
    public Path normalize() {
        if (getFileSystem().normalizePaths()) {
            return super.normalize();
        } else {
            return this;
        }
    }

    @Override
    public Path relativize(final Path other) {
        if (other.getFileSystem() != m_fileSystem) {
            throw new IllegalArgumentException("Cannot relativize paths across different file systems.");
        }

        if (this.equals(other)) {
            return getFileSystem().getPath("");
        }

        if (this.isAbsolute() != other.isAbsolute()) {
            throw new IllegalArgumentException("Cannot relativize an absolute path with a relative path.");
        }

        final S3Path s3Other = (S3Path)other;

        if (isEmptyPath()) {
            return s3Other;
        }

        if (isRoot()) {
            return getFileSystem().getPath(String.join(S3FileSystem.PATH_SEPARATOR, s3Other.m_pathParts));
        }

        if (s3Other.startsWith(this)) {
            return s3Other.subpath(getNameCount(), s3Other.getNameCount());
        }

        if (!getFileSystem().normalizePaths()) {
            throw new IllegalArgumentException("Cannot relativize independent paths if normalization is disabled.");
        }

        return super.relativize(other);
    }


    /**
     * @return an {@link S3Path} for which {@link #isDirectory()} returns true.
     */
    @Override
    public S3Path toDirectoryPath() {
        return (S3Path) super.toDirectoryPath();
    }
}
