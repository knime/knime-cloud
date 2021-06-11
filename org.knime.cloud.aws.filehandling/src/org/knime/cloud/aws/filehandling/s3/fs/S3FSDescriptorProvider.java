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
 *   2021-06-02 (modithahewasinghage): created
 */
package org.knime.cloud.aws.filehandling.s3.fs;

import org.knime.cloud.aws.filehandling.s3.testing.S3FSTestInitializerProvider;
import org.knime.cloud.aws.filehandling.s3.uriexporter.S3SignedURIExporterFactory;
import org.knime.cloud.aws.filehandling.s3.uriexporter.S3URIExporterFactory;
import org.knime.filehandling.core.connections.meta.FSDescriptorProvider;
import org.knime.filehandling.core.connections.meta.FSType;
import org.knime.filehandling.core.connections.meta.FSTypeRegistry;
import org.knime.filehandling.core.connections.meta.base.BaseFSDescriptor;
import org.knime.filehandling.core.connections.meta.base.BaseFSDescriptorProvider;
import org.knime.filehandling.core.connections.uriexport.URIExporterIDs;

/**
 * {@link FSDescriptorProvider} implementation for the Amazon S3 file system.
 *
 * @author modithahewasinghage
 */
public class S3FSDescriptorProvider extends BaseFSDescriptorProvider {

    /**
     * FSType for the S3 file system
     */
    public static final FSType FS_TYPE = FSTypeRegistry.getOrCreateFSType("amazon-s3", "Amazon S3");

    /**
     * Constructor.
     */
    public S3FSDescriptorProvider() {
        super(S3FSDescriptorProvider.FS_TYPE, //
            new BaseFSDescriptor.Builder() //
                .withSeparator(S3FileSystem.PATH_SEPARATOR) //
                .withConnectionFactory(S3FSConnection::new) //
                .withURIExporterFactory(URIExporterIDs.DEFAULT, S3URIExporterFactory.getInstance()) //
                .withURIExporterFactory(URIExporterIDs.DEFAULT_HADOOP, S3URIExporterFactory.getInstance()) //
                .withURIExporterFactory(S3URIExporterFactory.EXPORTER_ID, S3URIExporterFactory.getInstance()) //
                .withURIExporterFactory(S3SignedURIExporterFactory.EXPORTER_ID,
                    S3SignedURIExporterFactory.getInstance()) //
                .withTestInitializerProvider(new S3FSTestInitializerProvider()) //
                .build());
    }
}
