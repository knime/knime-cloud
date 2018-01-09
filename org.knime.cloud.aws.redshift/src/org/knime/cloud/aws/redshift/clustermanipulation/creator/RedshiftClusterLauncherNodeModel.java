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
 *   Jul 31, 2016 (budiyanto): created
 */
package org.knime.cloud.aws.redshift.clustermanipulation.creator;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.knime.cloud.aws.redshift.clustermanipulation.util.RedshiftClusterUtility;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication.AuthenticationType;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.flowvariable.FlowVariablePortObject;
import org.knime.core.node.port.flowvariable.FlowVariablePortObjectSpec;
import org.knime.core.node.workflow.CredentialsProvider;
import org.knime.core.node.workflow.ICredentials;
import org.knime.core.util.Pair;

import com.amazonaws.services.redshift.AmazonRedshift;
import com.amazonaws.services.redshift.AmazonRedshiftClient;
import com.amazonaws.services.redshift.model.Cluster;
import com.amazonaws.services.redshift.model.ClusterAlreadyExistsException;
import com.amazonaws.services.redshift.model.CreateClusterRequest;
import com.amazonaws.services.redshift.model.DescribeClustersRequest;
import com.amazonaws.services.redshift.model.DescribeClustersResult;
import com.amazonaws.services.redshift.model.Endpoint;
import com.amazonaws.services.redshift.model.UnauthorizedOperationException;

/**
 * Create an amazon Redshift cluster given all it's specifications
 *
 * @author Ole Ostergaard, KNIME.com
 */
class RedshiftClusterLauncherNodeModel extends NodeModel {

    /** The endpoints authentification credentials */
    protected final RedshiftClusterLauncherNodeSettings m_settings = createNodeSettings();

    static RedshiftClusterLauncherNodeSettings createRedshiftConnectionModel() {
        return new RedshiftClusterLauncherNodeSettings(AmazonRedshift.ENDPOINT_PREFIX);
    }

    static ArrayList<String> getClusterTypes() {
        return new ArrayList<>(
            Arrays.asList("dc1.large", "dc1.8xlarge", "ds2.xlarge", "ds2.8xlarge", "ds1.xlarge", "ds1.8xlarge"));
    }

    /** Keyword for single-node operation */
    protected static String SINGLE_NODE = "single-node";

    /** Keyword for multi-node operation */
    protected static String MULTI_NODE = "multi-node";

    /**
     * @return the {@link SettingsModelAuthentication} for RedshiftClusterLauncherNodeModel
     */
    protected static RedshiftClusterLauncherNodeSettings createNodeSettings() {
        return new RedshiftClusterLauncherNodeSettings(AmazonRedshift.ENDPOINT_PREFIX);
    }

    static HashMap<AuthenticationType, Pair<String, String>> getNameMap() {
        final HashMap<AuthenticationType, Pair<String, String>> nameMap = new HashMap<>();
        nameMap.put(AuthenticationType.USER_PWD, new Pair<String, String>("Access Key ID and Secret Key",
            "Access Key ID and Secret Access Key based authentication"));
        nameMap.put(AuthenticationType.KERBEROS, new Pair<String, String>("Default Credential Provider Chain",
            "Use the Default Credential Provider Chain for authentication"));
        return nameMap;
    }

    /**
     * Constructor for the node model.
     */
    protected RedshiftClusterLauncherNodeModel() {
        super(new PortType[]{}, new PortType[]{FlowVariablePortObject.TYPE});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        AmazonRedshiftClient client = RedshiftClusterUtility.getClient(m_settings, getCredentialsProvider());

        CreateClusterRequest request = createClusterRequest();

        try {
            exec.setMessage("Cluster creation requested");
            Cluster response = client.createCluster(request);
            String status = response.getClusterStatus();
            exec.setMessage("Waiting for cluster availability");
            while (!status.equalsIgnoreCase("available")) {
                exec.checkCanceled();
                Thread.sleep(m_settings.getPollingInterval());
                response = getClusterRespone(client, response.getClusterIdentifier());
                status = response.getClusterStatus();
                exec.setMessage("Cluster status: " + status);
            }
            Endpoint endpoint = response.getEndpoint();
            pushFlowvariables(endpoint.getAddress(), endpoint.getPort(), response.getDBName(),
                response.getClusterIdentifier());
        } catch (ClusterAlreadyExistsException e) {
            if (m_settings.failIfExists()) {
                Cluster response = getClusterRespone(client, m_settings.getClusterName());
                if (response.getClusterStatus().equals("deleting")) {
                    throw new InvalidSettingsException("Cluster " + m_settings.getClusterName() + " is being deleted",
                        e);
                } else {
                    throw new InvalidSettingsException("Cluster " + m_settings.getClusterName() + " alread exists.", e);
                }
            } else {
                Cluster response = getClusterRespone(client, m_settings.getClusterName());
                if (response.getClusterStatus().equals("deleting")) {
                    throw new InvalidSettingsException("Cluster " + m_settings.getClusterName() + " is being deleted",
                        e);
                }
                Endpoint endpoint = response.getEndpoint();
                pushFlowvariables(endpoint.getAddress(), endpoint.getPort(), response.getDBName(),
                    response.getClusterIdentifier());
            }
        } catch (UnauthorizedOperationException e) {
            throw new InvalidSettingsException("Check user permissons.", e);
        } catch (Exception e) {
            throw e;
        }

        return new PortObject[]{FlowVariablePortObject.INSTANCE};
    }

    /**
     * Creates the request for creating a cluster according to the settings.
     *
     * @return The request for creating the cluster
     */
    private CreateClusterRequest createClusterRequest() {

        String masterUser;
        String masterPW;
        if (m_settings.getClusterCredentials().getAuthenticationType().equals(AuthenticationType.CREDENTIALS)) {
            CredentialsProvider credentialsProvider = getCredentialsProvider();
            ICredentials iCredentials = credentialsProvider.get(m_settings.getClusterCredentials().getCredential());
            masterUser = iCredentials.getLogin();
            masterPW = iCredentials.getPassword();
        } else {
            masterUser = m_settings.getMasterName();
            masterPW = m_settings.getMasterPassword();
        }

        final String clusterName = m_settings.getClusterName();
        final String clusterType = (m_settings.getNodeNumber() > 1) ? MULTI_NODE : SINGLE_NODE;

        CreateClusterRequest request = new CreateClusterRequest().withClusterIdentifier(clusterName)
            .withMasterUsername(masterUser).withMasterUserPassword(masterPW).withNodeType(m_settings.getNodeType())
            .withClusterType(clusterType).withDBName(m_settings.getDefaultDBName()).withPort(m_settings.getPort());

        if (clusterType.equals(MULTI_NODE)) {
            request.withNumberOfNodes(m_settings.getNodeNumber());
        }
        return request;
    }

    private Cluster getClusterRespone(final AmazonRedshiftClient client, final String clusterName) {
        DescribeClustersResult describeClusters =
            client.describeClusters(new DescribeClustersRequest().withClusterIdentifier(m_settings.getClusterName()));
        List<Cluster> clusters = describeClusters.getClusters();
        return clusters.get(0);
    }

    /**
     * Pushes the given information to the flowvariables.
     *
     * @param hostname The hostname to be push to the flowvariable
     * @param port The port to be push to the flowvariable
     * @param defaultDB The default database name to be push to the flowvariable
     * @param clusterName the cluster name to be pushed to the flowvariable
     */
    protected void pushFlowvariables(final String hostname, final Integer port, final String defaultDB,
        final String clusterName) {
        final Set<String> variables = getAvailableFlowVariables().keySet();
        String name = "redshift" + "Hostname";
        String postfix = "";
        if (variables.contains(name)) {
            int i = 2;
            postfix += "_";
            while (variables.contains(name + i)) {
                i++;
            }
            postfix += i;
        }
        pushFlowVariableString(AmazonRedshift.ENDPOINT_PREFIX + "Hostname" + postfix, hostname);
        pushFlowVariableInt(AmazonRedshift.ENDPOINT_PREFIX + "Port" + postfix, port);
        pushFlowVariableString(AmazonRedshift.ENDPOINT_PREFIX + "DatabaseName" + postfix, defaultDB);
        pushFlowVariableString(AmazonRedshift.ENDPOINT_PREFIX + "ClusterName" + postfix, clusterName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        return new PortObjectSpec[]{FlowVariablePortObjectSpec.INSTANCE};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        // nothing to do
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        // nothing to do
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_settings.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.validateSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.loadValidatedSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void reset() {
        // nothing to do
    }

}
