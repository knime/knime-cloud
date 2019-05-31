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
 *   May 28, 2019 (julian): created
 */
package org.knime.cloud.aws.mlservices.utils.connection;

import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObjectSpec;
import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.ModelContentRO;
import org.knime.core.node.ModelContentWO;
import org.knime.core.node.port.AbstractSimplePortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.PortTypeRegistry;
import org.knime.core.node.util.ViewUtils;

/**
 *
 * @author julian
 */
public class AmazonConnectionInformationPortObject extends AbstractSimplePortObject {

    /**
     * @noreference This class is not intended to be referenced by clients.
     */
    public static final class Serializer
        extends AbstractSimplePortObjectSerializer<AmazonConnectionInformationPortObject> {
    }

    private ConnectionInformationPortObjectSpec m_connectionInformationPOS;

    /**
     * Type of this port.
     */
    @SuppressWarnings("hiding")
    public static final PortType TYPE =
        PortTypeRegistry.getInstance().getPortType(AmazonConnectionInformationPortObject.class);

    /**
     * Type of this optional port.
     *
     * @since 3.0
     */
    @SuppressWarnings("hiding")
    public static final PortType TYPE_OPTIONAL =
        PortTypeRegistry.getInstance().getPortType(AmazonConnectionInformationPortObject.class, true);

    /**
     * Should only be used by the framework.
     */
    public AmazonConnectionInformationPortObject() {
        // Used by framework
    }

    /**
     * Creates a port object with the given connection information.
     *
     * @param connectionInformationPOS The spec wrapping the connection information.
     */
    public AmazonConnectionInformationPortObject(
        final ConnectionInformationPortObjectSpec connectionInformationPOS) {
        if (connectionInformationPOS == null) {
            throw new NullPointerException("Argument must not be null");
        }
        final ConnectionInformation connInfo = connectionInformationPOS.getConnectionInformation();
        if (connInfo == null) {
            throw new NullPointerException("Connection information must be set (is null)");
        }
        m_connectionInformationPOS = connectionInformationPOS;
    }

    /**
     * Returns the connection information contained by this port object.
     *
     * @return The content of this port object
     */
    public CloudConnectionInformation getConnectionInformation() {
        return (CloudConnectionInformation)m_connectionInformationPOS.getConnectionInformation();
    }

    @Override
    public String getSummary() {
        return m_connectionInformationPOS.getConnectionInformation().toString();
    }

    @Override
    public JComponent[] getViews() {
        final JPanel f = ViewUtils.getInFlowLayout(new JLabel(getSummary()));
        f.setName("Connection");
        return new JComponent[]{f};
    }

    @Override
    public PortObjectSpec getSpec() {
        return m_connectionInformationPOS;
    }

    @Override
    protected void save(final ModelContentWO model, final ExecutionMonitor exec) throws CanceledExecutionException {
        // nothing to save; all done in spec, which is saved separately
    }

    @Override
    protected void load(final ModelContentRO model, final PortObjectSpec spec, final ExecutionMonitor exec)
        throws InvalidSettingsException, CanceledExecutionException {
        m_connectionInformationPOS = (ConnectionInformationPortObjectSpec)spec;
    }

}
