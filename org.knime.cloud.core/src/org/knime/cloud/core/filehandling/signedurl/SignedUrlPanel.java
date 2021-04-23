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
 *   2021-02-16 (Ayaz Ali Qureshi, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.cloud.core.filehandling.signedurl;

import java.awt.GridBagLayout;

import javax.swing.Box;
import javax.swing.ButtonGroup;
import javax.swing.JPanel;
import javax.swing.JRadioButton;

import org.knime.cloud.core.filehandling.signedurl.SignedUrlConfig.ExpirationMode;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponentDuration;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.filehandling.core.connections.uriexport.URIExporter;
import org.knime.filehandling.core.connections.uriexport.base.BaseURIExporterPanel;
import org.knime.filehandling.core.util.GBCBuilder;
import org.knime.time.util.DialogComponentDateTimeSelection;
import org.knime.time.util.DialogComponentDateTimeSelection.DisplayOption;

/**
 * {@link BaseURIExporterPanel} implementation for {@link URIExporter}s that produce a kind of signed URL which expires
 * after a certain amount of time.
 * 
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class SignedUrlPanel extends BaseURIExporterPanel<SignedUrlConfig> {

    private static final long serialVersionUID = 1L;

    final SignedUrlConfig m_config; // NOSONAR not using serialization

    private final JRadioButton m_expirationDurationMode; // NOSONAR not using serialization
    
    private final JRadioButton m_expirationDateTimeMode; // NOSONAR not using serialization

    private final DialogComponentDuration m_expirationDuration; // NOSONAR not using serialization

    private final DialogComponentDateTimeSelection m_expirationDateTime; // NOSONAR not using serialization

    /**
     * Constructor.
     * 
     * @param config
     */
    public SignedUrlPanel(final SignedUrlConfig config) {
        super(new GridBagLayout(), config);

        m_config = config;

        m_expirationDurationMode = new JRadioButton("Expires after duration");
        m_expirationDateTimeMode = new JRadioButton("Expires on date");
        final ButtonGroup expirationModeGroup = new ButtonGroup();
        expirationModeGroup.add(m_expirationDurationMode);
        expirationModeGroup.add(m_expirationDateTimeMode);
        m_expirationDuration = new DialogComponentDuration(config.getExpirationDurationModel(), "", true);
        m_expirationDateTime = new DialogComponentDateTimeSelection(config.getExpirationDateTimeModel(), "",
            DisplayOption.SHOW_DATE_AND_TIME_AND_TIMEZONE);

        initLayout();
        m_expirationDurationMode.addActionListener(e -> updateExpirationMode());
        m_expirationDateTimeMode.addActionListener(e -> updateExpirationMode());
    }
    
    @SuppressWarnings("deprecation")
    private void updateExpirationMode() {
        if (m_expirationDurationMode.isSelected()) {
            m_config.getExpirationModeModel().setStringValue(ExpirationMode.DURATION.getActionCommand());
            m_expirationDuration.setEnabled(true);
        } else {
            m_config.getExpirationModeModel().setStringValue(ExpirationMode.DATE.getActionCommand());
            m_expirationDuration.setEnabled(false);
        }
    }

    private void initLayout() {
        final GBCBuilder gbc = new GBCBuilder();
        
        gbc.resetPos().anchorWest().fillNone().insetLeft(5);
        add(m_expirationDurationMode, gbc.build());
        
        gbc.incX().fillHorizontal().setWeightX(1);
        add(Box.createHorizontalGlue(), gbc.build());

        gbc.resetX().incY().fillNone().insetLeft(25);
        add(m_expirationDuration.getComponentPanel(), gbc.build());
        ((JPanel)m_expirationDuration.getComponentPanel().getComponent(0)).setBorder(null);
        
        gbc.incY().insetLeft(5);
        add(m_expirationDateTimeMode, gbc.build());
        
        gbc.incY().insetLeft(25);
        add(m_expirationDateTime.getComponentPanel(), gbc.build());
        ((JPanel)m_expirationDateTime.getComponentPanel().getComponent(0)).setBorder(null);
    }

    @Override
    protected void afterSettingsLoaded() {
        m_expirationDurationMode.setSelected(m_config.getExpirationMode() == ExpirationMode.DURATION);
        m_expirationDateTimeMode.setSelected(m_config.getExpirationMode() == ExpirationMode.DATE);
    }

    @Override
    protected void loadAdditionalSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
        throws NotConfigurableException {
        m_expirationDuration.loadSettingsFrom(settings, specs);
        m_expirationDateTime.loadSettingsFrom(settings, specs);
        m_config.loadSettingsForPanel(settings);
    }

    @Override
    protected void saveAdditionalSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_expirationDuration.saveSettingsTo(settings);
        m_expirationDateTime.saveSettingsTo(settings);
        m_config.saveSettingsForPanel(settings);
    }

}
