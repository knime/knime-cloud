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
 *   Aug 24, 2016 (oole): created
 */
package org.knime.cloud.aws;

import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.AbstractButton;
import javax.swing.BorderFactory;
import javax.swing.JPanel;

import org.knime.cloud.aws.ExpirationSettings.ExpirationMode;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponentButtonGroup;
import org.knime.core.node.defaultnodesettings.DialogComponentDate;
import org.knime.core.node.defaultnodesettings.DialogComponentDuration;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObjectSpec;

/**
 * Holds the components for cloud file picker's expiration component
 *
 * @author Ole Ostergaard, KNIME.com GmbH
 */
public class ExpirationComponents {

	protected final ExpirationSettings m_settings;

	private final DialogComponentDuration m_timeComp;
	private final DialogComponentDate m_dateComp;

	private final DialogComponentButtonGroup m_buttonComp;

	/**
	 * Constructor
	 * @param settings The corresponding {@link ExpirationSettings}
	 */
	public ExpirationComponents (final ExpirationSettings settings) {
		m_settings = settings;
		m_timeComp = new DialogComponentDuration(m_settings.getTimeModel(), null);
		m_dateComp = new DialogComponentDate(m_settings.getDateModel(), null, false);
		m_buttonComp = new DialogComponentButtonGroup(m_settings.getButtonModel(), null, false, ExpirationMode.values());
	}

	/**
	 *
	 */
	private void setDefaultVisibility() {
		getTimePanel().setVisible(true);
		getDatePanel().setVisible(false);
	}

	protected JPanel getTimePanel() {
		return m_timeComp.getComponentPanel();
	}

	protected JPanel getDatePanel() {
		return m_dateComp.getComponentPanel();
	}

	protected JPanel getButtonPanel() {
		return m_buttonComp.getComponentPanel();
	}

	/**
	 * Return the {@link JPanel} holding the default dialog components for the expiration panel
	 *
	 * @return the expiration dialog panel
	 */
	public JPanel getDialogPanel() {
		final JPanel panel = new JPanel(new GridBagLayout());
		final JPanel rootPanel = createRootPanel();
		final GridBagConstraints gbc = new GridBagConstraints();
		panel.setBorder(BorderFactory.createTitledBorder(BorderFactory.createEtchedBorder(), "Expiration"));
		gbc.gridx = 0;
		gbc.gridy = 0;
		panel.add(getButtonPanel(), gbc);
		gbc.gridy++;
		panel.add(rootPanel, gbc);
		addButtonActions();
		return panel;
	}

	/**
	 *
	 */
	private void addButtonActions() {
		final AbstractButton timeButton = m_buttonComp.getButton(ExpirationMode.DURATION.name());
		timeButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				getDatePanel().setVisible(false);
				getTimePanel().setVisible(true);
			}
		});

		final AbstractButton dateButton = m_buttonComp.getButton(ExpirationMode.DATE.name());
		dateButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				getTimePanel().setVisible(false);
				getDatePanel().setVisible(true);
			}
		});

	}

	/**
	 * @return
	 */
	private JPanel createRootPanel() {
		final JPanel panel = new JPanel(new GridBagLayout());
		final GridBagConstraints gbc = new GridBagConstraints();
		gbc.gridx = 0;
		gbc.gridy = 0;
		gbc.weightx = 1;
		((JPanel)getDatePanel().getComponent(0)).setBorder(null);
		panel.add(getTimePanel(), gbc);
		panel.add(getDatePanel(), gbc);
		final Dimension originalSize = panel.getPreferredSize();
		panel.setMinimumSize(originalSize);
		panel.setPreferredSize(originalSize);
		setDefaultVisibility();
		return panel;
	}

	/**
	 * Load the settings for the default dialog components
	 *
	 * @param settings
	 * @param specs
	 * @throws NotConfigurableException
	 */
	public void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs) throws NotConfigurableException {
		m_timeComp.loadSettingsFrom(settings, specs);
		m_dateComp.loadSettingsFrom(settings, specs);
		m_buttonComp.loadSettingsFrom(settings, specs);
		if (((SettingsModelString)m_buttonComp.getModel()).getStringValue().equals(ExpirationMode.DURATION.name())) {
			getTimePanel().setVisible(true);
			getDatePanel().setVisible(false);
		}
		if (((SettingsModelString)m_buttonComp.getModel()).getStringValue().equals(ExpirationMode.DATE.name())) {
			getTimePanel().setVisible(false);
			getDatePanel().setVisible(true);
		}
	}

	/**
	 * Save the settings for the default dialog components
	 * @param settings
	 * @throws InvalidSettingsException
	 */
	public void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
		m_timeComp.saveSettingsTo(settings);
		m_dateComp.saveSettingsTo(settings);
		m_buttonComp.saveSettingsTo(settings);
	}
}
