/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME GmbH, Konstanz, Germany
 *  Website: http://www.knime.org; Email: contact@knime.org
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
 *  KNIME and ECLIPSE being a combined program, KNIME GMBH herewith grants
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
package org.knime.cloud.aws;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JSpinner;
import javax.swing.JTextField;
import javax.swing.SpinnerNumberModel;
import javax.swing.border.EtchedBorder;
import javax.swing.border.TitledBorder;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponent;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.workflow.CredentialsProvider;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;

/**
 * Dialog component that allow users to enter information needed to connect to
 * AWS
 *
 * @author Budi Yanto, KNIME.com
 */
public final class DialogComponentAWSConnectionInformation extends DialogComponent {

	private final JCheckBox m_useWorkflowCredential = new JCheckBox();

	private final JComboBox<String> m_workflowCredentials = new JComboBox<String>();

	private final JPanel m_workflowCredentialPanel;

	private final JLabel m_accessKeyIdLabel = new JLabel("Access Key ID:");

	private final JTextField m_accessKeyId = new JTextField();

	private final JLabel m_secretAccessKeyLabel = new JLabel("Secret Access Key:");

	private final JPasswordField m_secretAccessKey = new JPasswordField();

	private final JLabel m_regionLabel = new JLabel("Region:");

	private final JComboBox<String> m_region = new JComboBox<String>();

	private final JSpinner m_timeout = new JSpinner(
			new SpinnerNumberModel(SettingsModelAWSConnectionInformation.DEF_TIMEOUT, 0, Integer.MAX_VALUE, 100));

	/* Enum representing each component that can be updated */
	private enum COMPONENT {
		USE_WORKFLOW_CREDENTIAL, WORKFLOW_CREDENTIAL, ACCESS_KEY_ID, SECRET_ACCESS_KEY, REGION, TIMEOUT
	}

	public DialogComponentAWSConnectionInformation(final SettingsModelAWSConnectionInformation model) {
		super(model);
		getComponentPanel().setLayout(new GridBagLayout());
		final GridBagConstraints gbc = new GridBagConstraints();
		resetGBC(gbc);
		gbc.weightx = 1;
		m_workflowCredentialPanel = createWorkflowCredentialPanel();
		getComponentPanel().add(m_workflowCredentialPanel, gbc);
		gbc.gridy++;
		getComponentPanel().add(createAWSSettingsPanel(), gbc);

		m_useWorkflowCredential.addItemListener(new ItemListener() {
			@Override
			public void itemStateChanged(final ItemEvent e) {
				updateModel(COMPONENT.USE_WORKFLOW_CREDENTIAL);
			}
		});

		m_workflowCredentials.addItemListener(new ItemListener() {
			@Override
			public void itemStateChanged(final ItemEvent e) {
				if (e.getStateChange() == ItemEvent.SELECTED) {
					updateModel(COMPONENT.WORKFLOW_CREDENTIAL);
				}
			}
		});

		/* Adds change listener */
		m_accessKeyId.getDocument().addDocumentListener(new DocumentListener() {
			@Override
			public void removeUpdate(final DocumentEvent e) {
				updateModel(COMPONENT.ACCESS_KEY_ID);
			}

			@Override
			public void insertUpdate(final DocumentEvent e) {
				updateModel(COMPONENT.ACCESS_KEY_ID);
			}

			@Override
			public void changedUpdate(final DocumentEvent e) {
				updateModel(COMPONENT.ACCESS_KEY_ID);
			}
		});

		m_secretAccessKey.getDocument().addDocumentListener(new DocumentListener() {
			@Override
			public void removeUpdate(final DocumentEvent e) {
				updateModel(COMPONENT.SECRET_ACCESS_KEY);
			}

			@Override
			public void insertUpdate(final DocumentEvent e) {
				updateModel(COMPONENT.SECRET_ACCESS_KEY);
			}

			@Override
			public void changedUpdate(final DocumentEvent e) {
				updateModel(COMPONENT.SECRET_ACCESS_KEY);
			}
		});

		m_region.addItemListener(new ItemListener() {
			@Override
			public void itemStateChanged(final ItemEvent e) {
				if (e.getStateChange() == ItemEvent.SELECTED) {
					updateModel(COMPONENT.REGION);
				}
			}
		});

		m_timeout.addChangeListener(new ChangeListener() {
			@Override
			public void stateChanged(final ChangeEvent e) {
				updateModel(COMPONENT.TIMEOUT);
			}
		});

		// Not able to use prependChangeListener
		getModel().addChangeListener(new ChangeListener() {

			@Override
			public void stateChanged(final ChangeEvent e) {
				updateComponent();
			}
		});

		// call this method to be in sync with the settings model
		updateComponent();
	}

	private JPanel createWorkflowCredentialPanel() {
		final JPanel panel = new JPanel(new GridBagLayout());
		final GridBagConstraints gbc = new GridBagConstraints();
		resetGBC(gbc);
		panel.add(m_useWorkflowCredential, gbc);
		gbc.gridx++;
		gbc.weightx = 1;
		panel.add(m_workflowCredentials, gbc);
		panel.setBorder(new TitledBorder(new EtchedBorder(), "Workflow Credentials"));
		return panel;

	}

	private JPanel createAWSSettingsPanel() {
		final JPanel panel = new JPanel(new GridBagLayout());
		final GridBagConstraints gbc = new GridBagConstraints();
		resetGBC(gbc);

		/* Access Key ID */
		panel.add(m_accessKeyIdLabel, gbc);
		gbc.gridx++;
		gbc.weightx = 1;
		panel.add(m_accessKeyId, gbc);

		/* Secret Access Key */
		gbc.gridx = 0;
		gbc.weightx = 0;
		gbc.gridy++;
		panel.add(m_secretAccessKeyLabel, gbc);
		gbc.gridx++;
		gbc.weightx = 1;
		panel.add(m_secretAccessKey, gbc);

		/* Region */
		gbc.gridx = 0;
		gbc.weightx = 0;
		gbc.gridy++;
		panel.add(m_regionLabel, gbc);
		gbc.gridx++;
		gbc.weightx = 1;
		panel.add(m_region, gbc);

		/* Timeout */
		gbc.gridx = 0;
		gbc.weightx = 0;
		gbc.gridy++;
		panel.add(new JLabel("Timeout:   "), gbc);
		gbc.gridx++;
		gbc.fill = GridBagConstraints.NONE;
		panel.add(m_timeout, gbc);

		return panel;
	}

	@Override
	protected void updateComponent() {
		// only update component if values are off
		final SettingsModelAWSConnectionInformation model = (SettingsModelAWSConnectionInformation) getModel();
		setEnabledComponents(model.isEnabled());

		if (m_workflowCredentials.getItemCount() > 0) {
			if (m_useWorkflowCredential.isSelected() != model.useWorkflowCredential()) {
				m_useWorkflowCredential.setSelected(model.useWorkflowCredential());
			}
		} else {
			if (model.useWorkflowCredential() || m_useWorkflowCredential.isSelected()) {
				m_useWorkflowCredential.setSelected(false);
			}
		}

		if ((m_workflowCredentials.getSelectedItem() != null)
				&& (!((String) m_workflowCredentials.getSelectedItem()).equals(model.getWorkflowCredential()))) {
			m_workflowCredentials.setSelectedItem(model.getWorkflowCredential());
		}

		if (!m_accessKeyId.getText().equals(model.getAccessKeyId())) {
			m_accessKeyId.setText(model.getAccessKeyId());
		}

		if (model.getSecretAccessKey() != null) {
			final char[] secretKey = m_secretAccessKey.getPassword();
			String secretAccessKey = null;
			if (secretKey != null && secretKey.length > 0) {
				secretAccessKey = new String(secretKey);
			}
			if (!Objects.equals(secretAccessKey, model.getSecretAccessKey())) {
				m_secretAccessKey.setText(model.getSecretAccessKey());
			}
		}

		if ((m_region.getSelectedItem() != null) && (m_region.getSelectedItem().equals(model.getRegion()))) {
			m_region.setSelectedItem(model.getRegion());
		}

		try {
			m_timeout.commitEdit();
			final int val = ((Integer) m_timeout.getValue()).intValue();
			if (val != model.getTimeout()) {
				m_timeout.setValue(Integer.valueOf(model.getTimeout()));
			}
		} catch (final ParseException ex) {
			m_timeout.setValue(Integer.valueOf(model.getTimeout()));
		}

		updateEnabledState();
	}

	private void updateEnabledState() {
		// Check if credentials are available
		final boolean credentialsAvailable = m_workflowCredentials.getItemCount() > 0;
		m_workflowCredentialPanel.setEnabled(credentialsAvailable);
		m_useWorkflowCredential.setEnabled(credentialsAvailable);
		m_workflowCredentials.setEnabled(credentialsAvailable && m_useWorkflowCredential.isSelected());

		// Only enable manual input if workflow credentials are not used
		m_accessKeyIdLabel.setEnabled(!m_useWorkflowCredential.isSelected());
		m_accessKeyId.setEnabled(!m_useWorkflowCredential.isSelected());
		m_secretAccessKeyLabel.setEnabled(!m_useWorkflowCredential.isSelected());
		m_secretAccessKey.setEnabled(!m_useWorkflowCredential.isSelected());

	}

	@Override
	protected void validateSettingsBeforeSave() throws InvalidSettingsException {
		updateModel(COMPONENT.USE_WORKFLOW_CREDENTIAL);
		updateModel(COMPONENT.WORKFLOW_CREDENTIAL);
		updateModel(COMPONENT.ACCESS_KEY_ID);
		updateModel(COMPONENT.SECRET_ACCESS_KEY);
		updateModel(COMPONENT.REGION);
		updateModel(COMPONENT.TIMEOUT);
	}

	@Override
	protected void checkConfigurabilityBeforeLoad(final PortObjectSpec[] specs) throws NotConfigurableException {
		// we're always good.
	}

	@Override
	protected void setEnabledComponents(final boolean enabled) {
		m_useWorkflowCredential.setEnabled(enabled);
		m_workflowCredentials.setEnabled(enabled);
		m_accessKeyId.setEnabled(enabled);
		m_secretAccessKey.setEnabled(enabled);
		m_region.setEnabled(enabled);
		m_timeout.setEnabled(enabled);
	}

	@Override
	public void setToolTipText(final String text) {
		// TODO Auto-generated method stub

	}

	private void updateModel(final COMPONENT comp) {
		final SettingsModelAWSConnectionInformation model = (SettingsModelAWSConnectionInformation) getModel();
		boolean useWorkflowCredential = model.useWorkflowCredential();
		String workflowCredential = model.getWorkflowCredential();
		String accessKeyId = model.getAccessKeyId();
		String secretAccessKey = model.getSecretAccessKey();
		String region = model.getRegion();
		int timeout = model.getTimeout();
		switch (comp) {
		case USE_WORKFLOW_CREDENTIAL:
			useWorkflowCredential = m_useWorkflowCredential.isSelected();
			break;
		case WORKFLOW_CREDENTIAL:
			workflowCredential = (String) m_workflowCredentials.getSelectedItem();
			break;
		case ACCESS_KEY_ID:
			accessKeyId = m_accessKeyId.getText();
			break;
		case SECRET_ACCESS_KEY:
			final char[] secretKey = m_secretAccessKey.getPassword();
			if (secretKey == null) {
				secretAccessKey = null;
			} else {
				secretAccessKey = new String(secretKey);
			}
			break;
		case REGION:
			region = (String) m_region.getSelectedItem();
			break;
		case TIMEOUT:
			try {
				m_timeout.commitEdit();
				timeout = ((Integer) m_timeout.getValue()).intValue();
			} catch (final ParseException ex) {
				// Use default timeout if exception occurs
				timeout = SettingsModelAWSConnectionInformation.DEF_TIMEOUT;
			}
			break;
		default:
			throw new IllegalStateException("Unknown component");
		}

		model.setValues(useWorkflowCredential, workflowCredential, accessKeyId, secretAccessKey, region, timeout);
	}

	/**
	 * @param settings
	 * @param specs
	 * @param cp
	 * @throws NotConfigurableException
	 */
	public void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs,
			final CredentialsProvider cp) throws NotConfigurableException {

		super.loadSettingsFrom(settings, specs);
		loadCredentials(cp);
		loadRegions();
	}

	/**
	 * Loads items in credentials select box.
	 */
	private void loadCredentials(final CredentialsProvider cp) {
		final String savedCredential = ((SettingsModelAWSConnectionInformation) getModel()).getWorkflowCredential();
		boolean containedCredential = false;
		m_workflowCredentials.removeAllItems();
		final Collection<String> names = cp.listNames();
		if (names != null) {
			for (final String option : names) {
				m_workflowCredentials.addItem(option);
				if (Objects.equals(savedCredential, option)) {
					containedCredential = true;
				}
			}
		}

		if (savedCredential != null && containedCredential) {
			m_workflowCredentials.setSelectedItem(savedCredential);
		}
	}

	private void loadRegions() {
		final SettingsModelAWSConnectionInformation model = (SettingsModelAWSConnectionInformation) getModel();
		final String savedRegion = model.getRegion();
		final String defaultRegion = Regions.DEFAULT_REGION.getName();
		m_region.removeAllItems();
		boolean containedSavedRegion = false;
		boolean containedDefaultRegion = false;
		final List<String> regionNames = new ArrayList<String>();
		for (final Regions regions : Regions.values()) {
			final Region region = Region.getRegion(regions);
			if (region.isServiceSupported(model.getEndpointPrefix())) {
				final String reg = region.getName();
				regionNames.add(reg);
				if (Objects.equals(savedRegion, reg)) {
					containedSavedRegion = true;
				}
				if (Objects.equals(defaultRegion, reg)) {
					containedDefaultRegion = true;
				}
			}
		}

		Collections.sort(regionNames);
		for (final String reg : regionNames) {
			m_region.addItem(reg);
		}

		// Select the saved region if it exists, if not, then select the default
		// region
		if (savedRegion != null && containedSavedRegion) {
			m_region.setSelectedItem(savedRegion);
		} else if (defaultRegion != null && containedDefaultRegion) {
			m_region.setSelectedItem(defaultRegion);
		}
	}

	/**
	 * Reset the grid bag constraints to useful defaults.
	 *
	 *
	 * The defaults are all insets to 5, anchor northwest, fill both, x and y 0
	 * and x and y weight 0.
	 *
	 * @param gbc
	 *            The constraints object.
	 */
	private static void resetGBC(final GridBagConstraints gbc) {
		gbc.insets = new Insets(5, 5, 5, 5);
		gbc.anchor = GridBagConstraints.NORTHWEST;
		gbc.fill = GridBagConstraints.BOTH;
		gbc.weightx = 0;
		gbc.weighty = 0;
		gbc.gridx = 0;
		gbc.gridy = 0;
	}

}
