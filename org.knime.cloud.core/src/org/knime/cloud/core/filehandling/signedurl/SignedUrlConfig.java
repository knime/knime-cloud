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
 *   Feb 24, 2021 (Ayaz Ali Qureshi, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.cloud.core.filehandling.signedurl;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.function.Consumer;

import org.knime.cloud.core.util.ExpirationSettings;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.SettingsModelDuration;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.util.ButtonGroupEnumInterface;
import org.knime.core.node.util.CheckUtils;
import org.knime.filehandling.core.connections.uriexport.URIExporterConfig;
import org.knime.filehandling.core.defaultnodesettings.status.StatusMessage;
import org.knime.time.util.SettingsModelDateTime;

/**
 * Boilerplate implementation of an URI Exporter which require expiration settings
 * 
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public final class SignedUrlConfig implements URIExporterConfig {
    
    private static final String CFG_EXPIRATION_MODE = "expiration_mode";
    
    private static final String CFG_EXPIRATION_DURATION = "expiration_duration";
    
    private static final String CFG_EXPIRATION_DATETIME = "expiration_datetime";
    
    private final Duration m_maximumDuration;
    
    private final SettingsModelDuration m_expirationDuration;
    
    private final SettingsModelDateTime m_expirationDateTime;
    
    private final SettingsModelString m_expirationMode;
    
    /**
     * Constructor.
     * @param maximumExpiryTime The expiration duration that the user can select.
     */
    public SignedUrlConfig(final Duration maximumExpiryTime) {
        m_maximumDuration = maximumExpiryTime;
        m_expirationDuration = new SettingsModelDuration(CFG_EXPIRATION_DURATION, Duration.ofMinutes(60));
        m_expirationDateTime = new SettingsModelDateTime(CFG_EXPIRATION_DATETIME, ZonedDateTime.now().plusMinutes(60));
        m_expirationMode = new SettingsModelString(CFG_EXPIRATION_MODE, ExpirationMode.DURATION.getActionCommand());
        m_expirationMode.addChangeListener(e -> updateEnabledness());
        updateEnabledness();
    }
    
    private void updateEnabledness() {
        m_expirationDuration.setEnabled(getExpirationMode() == ExpirationMode.DURATION);
        m_expirationDateTime.setEnabled(getExpirationMode() == ExpirationMode.DATE);
    }

    /**
     * @return the expirationDuration
     */
    public SettingsModelDuration getExpirationDurationModel() {
        return m_expirationDuration;
    }

    /**
     * @return the expirationDateTime
     */
    public SettingsModelDateTime getExpirationDateTimeModel() {
        return m_expirationDateTime;
    }

    /**
     * @return the expirationMode
     */
    public SettingsModelString getExpirationModeModel() {
        return m_expirationMode;
    }
    
    /**
     * @return the expirationDuration
     */
    public Duration getExpirationDuration() {
        return m_expirationDuration.getDuration();
    }

    /**
     * @return the expirationDateTime
     */
    public ZonedDateTime getExpirationDateTime() {
        return m_expirationDateTime.getZonedDateTime();
    }

    /**
     * @return the expirationMode
     */
    public ExpirationMode getExpirationMode() {
        if (m_expirationMode.getStringValue().equals(ExpirationMode.DURATION.getActionCommand())) {
            return ExpirationMode.DURATION;
        } else {
            return ExpirationMode.DATE;
        }
    }

    @Override
    public void loadSettingsForPanel(final NodeSettingsRO settings) throws NotConfigurableException {
        try {
            m_expirationMode.loadSettingsFrom(settings);
        } catch (InvalidSettingsException e) {
            throw new NotConfigurableException(e.getMessage(), e);
        }
    }

    @Override
    public void loadSettingsForExporter(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_expirationMode.loadSettingsFrom(settings);
        m_expirationDuration.loadSettingsFrom(settings);
        m_expirationDateTime.loadSettingsFrom(settings);
    }

    @Override
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_expirationMode.validateSettings(settings);
        m_expirationDuration.validateSettings(settings);
        m_expirationDateTime.validateSettings(settings);
    }

    @Override
    public void validate() throws InvalidSettingsException {
        CheckUtils.checkSetting(getValidityDuration().getSeconds() >= 1, "URL must be valid for at least one second");
        CheckUtils.checkSetting(getValidityDuration().minus(m_maximumDuration).toSeconds() <= 0, "URL can be valid for max. " + formatDuration(m_maximumDuration));
    }
    
    private static String formatDuration(Duration duration) {
        Duration remaining = duration;
        
        final long days = remaining.toDays();
        remaining = remaining.minusDays(days);
        
        final long hours = remaining.toHours();
        remaining = remaining.minusHours(hours);
        
        final long minutes = remaining.toMinutes();
        remaining = remaining.minusMinutes(minutes);
        
        final long secs = remaining.toSeconds();
        remaining = remaining.minusSeconds(secs);

        return String.format("%d days, %d hours, %d minutes %d seconds", days, hours, minutes, secs);
    }

    @Override
    public void saveSettingsForPanel(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_expirationMode.saveSettingsTo(settings);
    }

    @Override
    public void saveSettingsForExporter(final NodeSettingsWO settings) {
        m_expirationMode.saveSettingsTo(settings);
        m_expirationDuration.saveSettingsTo(settings);
        m_expirationDateTime.saveSettingsTo(settings);
    }

    @Override
    public void configureInModel(PortObjectSpec[] specs, Consumer<StatusMessage> statusMessageConsumer)
        throws InvalidSettingsException {
        validate();
    }

    /**
     * Calculates the Duration against which the URL will be generated
     *
     * @return An instance of Duration representing the user selected time
     */
    public Duration getValidityDuration() {
        if (getExpirationMode() == ExpirationMode.DURATION) {
            return m_expirationDuration.getDuration();
        } else {
            return Duration.between(ZonedDateTime.now(), m_expirationDateTime.getZonedDateTime());
        }
    }
    
    /**
     * This {@link ButtonGroupEnumInterface} implementation holds the possible expiration methods for the {@link ExpirationSettings}
     *
     * @author Bjoern Lohrmann, KNIME GmbH
     */
    public enum ExpirationMode implements ButtonGroupEnumInterface {
        
        /**
         * Mode where signed URLs expire after a certain amount of time after being generated. 
         */
        DURATION("After Duration", "Expiration after a given duration"),
        
        /**
         * Mode where signed URLs after a fixed point in time. 
         */
        DATE("After date and time", "Expiration after a fixed date and time");

        private String m_label;

        private String m_desc;

        private ExpirationMode(final String label, final String desc) {
            m_label = label;
            m_desc = desc;
        }

        @Override
        public String getText() {
            return m_label;
        }

        @Override
        public String getActionCommand() {
            return name();
        }

        @Override
        public String getToolTip() {
            return m_desc;
        }

        @Override
        public boolean isDefault() {
            return DURATION == this;
        }
    }

}
