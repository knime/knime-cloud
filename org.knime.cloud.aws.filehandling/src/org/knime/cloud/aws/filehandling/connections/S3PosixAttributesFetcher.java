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
 *   12.02.2020 (Mareike Hoeger, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.cloud.aws.filehandling.connections;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.UserPrincipal;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.knime.filehandling.core.connections.base.attributes.BasePrincipal;
import org.knime.filehandling.core.connections.base.attributes.PosixAttributes;
import org.knime.filehandling.core.util.CheckedExceptionFunction;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.Grant;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.Permission;

/**
 * Fetches POSIX attributes from S3 for a given {@link S3Path}.
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class S3PosixAttributesFetcher implements CheckedExceptionFunction<Path, PosixAttributes, IOException> {

    /**
     * {@inheritDoc}
     */
    @Override
    public PosixAttributes apply(final Path t) throws IOException {
        if (!(t instanceof S3Path)) {
            throw new IllegalArgumentException(
                String.format("Function can only be applied to path of type %s", S3Path.class.getName()));
        }

        final S3Path path = (S3Path)t;
        final AmazonS3 client = path.getFileSystem().getClient();
        final AccessControlList acl = client.getObjectAcl(path.getBucketName(), path.getBlobName());

        final Owner s3Owner = acl.getOwner();

        final UserPrincipal owner = new BasePrincipal(s3Owner.getId() + ":" + s3Owner.getDisplayName());
        final Set<PosixFilePermission> permissions = convertToPosixPermissionsList(acl.getGrantsAsList());
        return new PosixAttributes(owner, null, permissions);
    }

    private static Set<PosixFilePermission> convertToPosixPermissionsList(final List<Grant> grants) {
        final Set<PosixFilePermission> permissions = new HashSet<>();
        for (final Grant grant : grants) {
            permissions.addAll(getPosixPermissionListForGrant(grant));
        }
        return permissions;
    }

    private static Set<PosixFilePermission> getPosixPermissionListForGrant(final Grant grant) {
        final Set<PosixFilePermission> permissions = new HashSet<>();
        final Permission permission = grant.getPermission();
        switch (permission) {
            case FullControl:
                permissions.add(PosixFilePermission.OWNER_EXECUTE);
                permissions.add(PosixFilePermission.OWNER_READ);
                permissions.add(PosixFilePermission.OWNER_WRITE);
                break;
            case Read:
            case ReadAcp:
                permissions.add(PosixFilePermission.OWNER_READ);
                break;
            case Write:
            case WriteAcp:
                permissions.add(PosixFilePermission.OWNER_EXECUTE);
                break;
            default:
                throw new IllegalStateException("Unknown grant permission: " + permission);
        }

        return permissions;
    }

}
