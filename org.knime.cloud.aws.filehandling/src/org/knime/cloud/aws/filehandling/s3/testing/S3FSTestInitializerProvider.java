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
 *   Jan 6, 2020 (Tobias Urhaug, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.cloud.aws.filehandling.s3.testing;

import java.io.IOException;
import java.util.Map;

import org.knime.cloud.aws.filehandling.s3.fs.S3FSConnection;
import org.knime.cloud.aws.filehandling.s3.fs.S3FileSystem;
import org.knime.cloud.aws.filehandling.s3.node.S3ConnectorNodeSettings;
import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.core.node.util.CheckUtils;
import org.knime.filehandling.core.connections.FSLocationSpec;
import org.knime.filehandling.core.testing.DefaultFSTestInitializerProvider;

/**
 * Initializer provider for s3. Reads all s3 relevant properties from the configuration and establishes a connection
 * to s3.
 *
 * @author Tobias Urhaug, KNIME GmbH, Berlin, Germany
 */
public class S3FSTestInitializerProvider extends DefaultFSTestInitializerProvider {

    @SuppressWarnings("resource")
    @Override
    public S3FSTestInitializer setup(final Map<String, String> config) throws IOException {

        validateConfiguration(config);

        final CloudConnectionInformation s3ConnectionInformation = createCloudConnectionInformation(config);

        final String workingDir =
            generateRandomizedWorkingDir(config.get("workingDirPrefix"), S3FileSystem.PATH_SEPARATOR);

        S3ConnectorNodeSettings settings = new S3ConnectorNodeSettings();
        settings.getWorkingDirectoryModel().setStringValue(workingDir);
        settings.getSocketTimeoutModel().setIntValue(CONNECTION_TIMEOUT);

        final S3FSConnection s3Connection = new S3FSConnection(s3ConnectionInformation, settings, null);

        return new S3FSTestInitializer(s3Connection);
    }

    private static CloudConnectionInformation createCloudConnectionInformation(final Map<String, String> config) {
        final CloudConnectionInformation s3ConnectionInformation = new CloudConnectionInformation();
        s3ConnectionInformation.setHost(config.get("region"));
        s3ConnectionInformation.setProtocol("s3");
        if (config.containsKey("roleSwitchAccount")) {
            s3ConnectionInformation.setSwitchRole(true);
            s3ConnectionInformation.setSwitchRoleAccount(config.get("roleSwitchAccount"));
            s3ConnectionInformation.setSwitchRoleName(config.get("roleSwitchName"));
        } else {
            s3ConnectionInformation.setSwitchRole(false);
            s3ConnectionInformation.setSwitchRoleAccount("");
            s3ConnectionInformation.setSwitchRoleName("");
        }
        s3ConnectionInformation.setUser(config.get("accessKeyId"));
        s3ConnectionInformation.setPassword(config.get("accessKeySecret"));
        s3ConnectionInformation.setTimeout(CONNECTION_TIMEOUT);
        return s3ConnectionInformation;
    }

    private static void validateConfiguration(final Map<String, String> config) {
        CheckUtils.checkArgumentNotNull(config.get("region"), "region must not be null");
        if (config.get("roleSwitchAccount") != null) {
            CheckUtils.checkArgumentNotNull(config.get("roleSwitchName"),
                "roleSwitchName must not be null if roleSwitchAccount is set");
        }
        CheckUtils.checkArgumentNotNull(config.get("accessKeyId"), "accessKeyId must not be null");
        CheckUtils.checkArgumentNotNull(config.get("accessKeySecret"), "accessKeySecret must not be null");
        CheckUtils.checkArgumentNotNull(config.get("workingDirPrefix"), "workingDirPrefix must not be null");
    }

    @Override
    public String getFSType() {
        return S3FileSystem.FS_TYPE;
    }

    @Override
    public FSLocationSpec createFSLocationSpec(final Map<String, String> config) {
        return S3FileSystem.createFSLocationSpec();
    }
}

