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
package org.knime.google.cloud.storage.node.filepicker;

import java.net.URI;
import java.util.Date;

import org.knime.base.filehandling.NodeUtils;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.files.Connection;
import org.knime.base.filehandling.remote.files.ConnectionMonitor;
import org.knime.base.filehandling.remote.files.RemoteFileFactory;
import org.knime.cloud.core.node.filepicker.AbstractFilePickerNodeModel;
import org.knime.core.node.NodeLogger;
import org.knime.google.cloud.storage.filehandler.GoogleCSRemoteFile;
import org.knime.google.cloud.storage.filehandler.GoogleCSRemoteFileHandler;

/**
 * Google Cloud Store File Picker Node Model.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class GoogleCSFilePickerNodeModel extends AbstractFilePickerNodeModel {

	private static final NodeLogger LOGGER = NodeLogger.getLogger(GoogleCSFilePickerNodeModel.class);

	static final String CFG_FILE_SELECTION = "fileSelection";

	private static final String FLOW_VARIABLE_NAME = "GoogleCSPickedFile";

	GoogleCSFilePickerNodeModel() {
		super(CFG_FILE_SELECTION,FLOW_VARIABLE_NAME);
	}

	@Override
	protected String getSignedURL(final ConnectionMonitor<? extends Connection> monitor, final ConnectionInformation connectionInformation) throws Exception {
		final URI uri = new URI(connectionInformation.toURI().toString() + NodeUtils.encodePath(getFileSelection()));

		final GoogleCSRemoteFile remoteFile =
				(GoogleCSRemoteFile) RemoteFileFactory.createRemoteFile(uri, connectionInformation, monitor);
		final Date expirationTime = getExpirationTime();
		final long expirationSeconds = (expirationTime.getTime()- new Date().getTime()) / 1000l;

		LOGGER.info("Generate signed URL with expiration time: " + expirationTime + " (" + expirationSeconds + "s)");
		return remoteFile.getSignedURL(expirationSeconds);
	}

	@Override
	protected String getEndpointPrefix() {
		return GoogleCSRemoteFileHandler.PROTOCOL.getName();
	}
}
