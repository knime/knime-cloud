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
 *   Apr 3, 2017 (oole): created
 */
package org.knime.cloud.aws.redshift.clustermanipulation.util;

import java.awt.Container;
import java.awt.Frame;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import javax.swing.DefaultComboBoxModel;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import javax.swing.text.JTextComponent;

import org.knime.base.filehandling.NodeUtils;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponent;
import org.knime.core.node.defaultnodesettings.SettingsModel;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.util.StringHistory;
import org.knime.core.node.workflow.CredentialsProvider;

import com.amazonaws.services.redshift.AmazonRedshiftClient;
import com.amazonaws.services.redshift.model.Cluster;
import com.amazonaws.services.redshift.model.DescribeClustersResult;

/**
 * A {@link DialogComponent} that allows to query existing Amazon Redshift cluster names from within a Dialog.
 *
 * @author Ole Ostergaard, KNIME.com
 * @param <S> The extended {@link RedshiftGeneralSettings}
 */
public class RedshiftClusterChooserComponent<S extends RedshiftGeneralSettings> extends DialogComponent {

    private final String m_historyID;

    private JLabel m_label;

    private JComboBox<String> m_combobox;

    private JButton m_selectButton;

    private boolean m_existingIntoCB = false;

    private CredentialsProvider m_credentialProvider;

    /**
     * Build the component and its functionality.
     *
     * @param model The {@link SettingsModelString} to save the selected or entered cluster name to
     * @param settings the settings for from the dialog
     * @param historyID the history id for the {@link JComboBox}
     * @param cp The credentials provider for the {@link AmazonRedshiftClient}
     */
    public RedshiftClusterChooserComponent(final SettingsModelString model, final S settings, final String historyID,
        final CredentialsProvider cp) {
        super(model);
        m_credentialProvider = cp;
        m_label = new JLabel(" Cluster name:    ");
        m_combobox = new JComboBox<String>(new String[0]);
        m_combobox.setEditable(true);
        m_combobox.setSize(20, 6);
        JTextComponent editorComponent = (JTextComponent)m_combobox.getEditor().getEditorComponent();
        editorComponent.getDocument().addDocumentListener(new DocumentListener() {

            @Override
            public void removeUpdate(final DocumentEvent e) {
                updateCB();
            }

            @Override
            public void insertUpdate(final DocumentEvent e) {
                updateCB();
            }

            @Override
            public void changedUpdate(final DocumentEvent e) {
                updateCB();
            }

            private void updateCB() {
                if (!m_existingIntoCB) {
                    m_combobox.setSelectedItem(editorComponent.getText());
                }
            }
        });
        m_historyID = historyID;
        m_selectButton = new JButton("Query...");
        m_selectButton.addActionListener(new ActionListener() {

            @Override
            public void actionPerformed(final ActionEvent e) {
                Frame frame = null;
                Container container = m_selectButton.getParent();
                while (container != null) {
                    if (container instanceof Frame) {
                        frame = (Frame)container;
                        break;
                    }
                    container = container.getParent();
                }
                String clusterName = "";
                AmazonRedshiftClient client = RedshiftClusterUtility.getClient(settings, m_credentialProvider);
                String msg = getMsg(client);
                String[] clusterArray = getClusterArray(client);
                clusterName = (String)JOptionPane.showInputDialog(frame, msg, "Choose existing cluster name",
                    JOptionPane.PLAIN_MESSAGE, null, clusterArray, null);

                if (clusterName != null) {
                    m_existingIntoCB = true;
                    StringHistory.getInstance(m_historyID).add(clusterName);
                    setSelection(clusterName);
                    updateHistory();
                    m_existingIntoCB = false;
                }
            }

        });
    }

    private String getMsg(final AmazonRedshiftClient client) {
        String msg = "Choose an existing cluster: ";
        try {
            DescribeClustersResult describeClusters = client.describeClusters();
            List<Cluster> clusters = describeClusters.getClusters();
            if (clusters.size() <= 0) {
                msg = "No clusters launched";
            }
        } catch (Exception e) {
            if (e.getMessage().contains("is not authorized")) {
                msg = "Check AWS user permissons.";
            } else {
                msg = e.getMessage();
            }
        }

        return msg;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JPanel getComponentPanel() {
        final JPanel panel = new JPanel(new GridBagLayout());
        final GridBagConstraints gbc = new GridBagConstraints();
        NodeUtils.resetGBC(gbc);
        gbc.insets = new Insets(0, 0, 0, 0);
        gbc.weightx = 1;
        panel.add(m_label, gbc);
        gbc.gridx++;
        panel.add(m_combobox, gbc);
        gbc.weightx = 0;
        gbc.fill = GridBagConstraints.NONE;
        gbc.gridx++;
        panel.add(m_selectButton, gbc);
        return panel;
    }

    /**
     * Returns an array of available clusters for a the given {@link AmazonRedshiftClient}.
     *
     * @param client the {@link AmazonRedshiftClient}
     * @return An array of available clusters
     */
    private String[] getClusterArray(final AmazonRedshiftClient client) {
        String[] clusterArray = new String[0];
        try {
            DescribeClustersResult describeClusters = client.describeClusters();
            List<Cluster> clusters = describeClusters.getClusters();
            Iterator<Cluster> iterator = clusters.iterator();
            ArrayList<String> clusterNames = new ArrayList<String>();
            while (iterator.hasNext()) {
                clusterNames.add(iterator.next().getClusterIdentifier());
            }
            clusterArray = clusterNames.toArray(new String[0]);
        } catch (Exception e) {
            // Do nothing
        }

        return clusterArray;
    }

    /**
     * Set the selection of this file chooser.
     *
     * @param selection The new selection
     */
    private void setSelection(final String selection) {
        m_combobox.setSelectedItem(selection);
    }

    /**
     * Update the history of the combo box.
     */
    private void updateHistory() {
        // Get history
        final StringHistory history = StringHistory.getInstance(m_historyID);
        // Get values
        final String[] strings = history.getHistory();
        // Make values unique through use of set
        final Set<String> set = new LinkedHashSet<String>();
        for (final String string : strings) {
            set.add(string);
        }
        // Remove old elements
        final DefaultComboBoxModel<String> model = (DefaultComboBoxModel<String>)m_combobox.getModel();
        model.removeAllElements();
        // Add new elements
        for (final String string : set) {
            model.addElement(string);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void updateComponent() {
        final SettingsModelString model = (SettingsModelString)getModel();
        setEnabledComponents(model.isEnabled());
        m_combobox.setSelectedItem(model.getStringValue());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettingsBeforeSave() throws InvalidSettingsException {
        ((SettingsModelString)getModel()).setStringValue((String)m_combobox.getSelectedItem());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void checkConfigurabilityBeforeLoad(final PortObjectSpec[] specs) throws NotConfigurableException {
        //
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void setEnabledComponents(final boolean enabled) {
        m_selectButton.setEnabled(enabled);
        m_combobox.setEnabled(enabled);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setToolTipText(final String text) {
        m_selectButton.setToolTipText(text);
        m_combobox.setToolTipText(text);
    }

    /**
     * Loads the settings and passes the necessary credentials to the dialog to enable querying existing cluster names.
     *
     * @param settings the settings to load from
     * @param specs the {@link PortObjectSpec} to load from
     * @param cp The nodes {@link CredentialsProvider}
     * @param settingsModel The actual {@link SettingsModel}
     * @throws NotConfigurableException
     */
    public void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs,
        final CredentialsProvider cp, final S settingsModel) throws NotConfigurableException {
        super.loadSettingsFrom(settings, specs);
        m_credentialProvider = cp;
    }
}
