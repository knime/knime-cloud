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

import java.util.Date;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.DialogComponentButtonGroup;
import org.knime.core.node.defaultnodesettings.SettingsModelDate;
import org.knime.core.node.defaultnodesettings.SettingsModelDuration;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.util.ButtonGroupEnumInterface;

/**
 * Holds the settings for cloud file picker's expiration component
 *
 * @author Ole Ostergaard, KNIME.com GmbH
 */
public class ExpirationSettings {

	private static final String DEFAULT_EXPIRY = "Time";
	private final SettingsModelDuration m_timeModel = createTimeModel();
	private final SettingsModelDate m_dateModel = createDateModel();
	private final SettingsModelString m_buttonModel = createButtonModel();

	/**
	 * Constructor.
	 */
	public ExpirationSettings() {
	}

	private SettingsModelDuration createTimeModel() {
		return new SettingsModelDuration("time", "#d##h##m##s", "0d01h00m00s");
	}

	private SettingsModelDate createDateModel() {
		return new SettingsModelDate("date");
	}


	private SettingsModelString createButtonModel() {
		return new SettingsModelString("buttons", DEFAULT_EXPIRY);
	}

	/**
	 * Save the default dialog component's settings
	 * @param settings
	 */
	public void saveSettingsTo(final NodeSettingsWO settings) {
		m_timeModel.saveSettingsTo(settings);
		m_dateModel.saveSettingsTo(settings);
		m_buttonModel.saveSettingsTo(settings);
	}

	/**
	 * Load the validated default dialog component's settings
	 * @param settings
	 * @throws InvalidSettingsException
	 */
	public void loadValidatedSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
		m_timeModel.loadSettingsFrom(settings);
		m_dateModel.loadSettingsFrom(settings);
		m_buttonModel.loadSettingsFrom(settings);
	}

	/**
	 * Validate the default dialog component's settings
	 * @param settings
	 * @throws InvalidSettingsException
	 */
	public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
		m_timeModel.validateSettings(settings);
		m_dateModel.validateSettings(settings);
		m_buttonModel.validateSettings(settings);
	}

	/**
	 * Get the date for expiration
	 * @return the date for expiration
	 */
	public Date getDate() {
		return m_dateModel.getDate();
	}

	/**
	 * Get the date for expiration in milliseconds
	 * @return the date for expiration in milliseconds
	 */
	public long getDateInMillis() {
		return m_dateModel.getTimeInMillis();
	}

	/**
	 * Get the time until expiration in milliseconds
	 * @return the time until expiration in milliseconds
	 */
	public long getTimeInMillis() {
		return m_timeModel.getDurationInMillis();
	}

	/**
	 * Get the chosen expiration mode
	 * @return the chosen expiration mode
	 */
	public String getExpirationMode() {
		return m_buttonModel.getStringValue();
	}

	/**
	 * The {@link SettingsModelDuration} for the {@link ExpirationComponents}
	 * @return this setting's {@link SettingsModelDuration}
	 */
	public SettingsModelDuration getTimeModel() {
		return m_timeModel;
	}

	/**
	 * The {@link SettingsModelDate} for the {@link ExpirationComponents}
	 * @return this setting's {@link SettingsModelDate}
	 */
	public SettingsModelDate getDateModel() {
		return m_dateModel;
	}

	/**
	 * The {@link SettingsModelString} for the {@link ExpirationComponents}'s {@link DialogComponentButtonGroup}
	 * @return this setting's {@link SettingsModelString} for the {@link DialogComponentButtonGroup}
	 */
	public SettingsModelString getButtonModel() {
		return m_buttonModel;
	}

	/**
	 * Set the date for the {@link SettingsModelDate} in milliseconds
	 * @param date the date to be set in milliseconds
	 */
	public void setDateInMillis(final long date) {
		m_dateModel.setTimeInMillis(date);
	}

	/**
	 * This {@link ButtonGroupEnumInterface} implementation holds the possible expiration methods for the {@link ExpirationSettings}
	 *
	 * @author Ole Ostergaard, KNIME.com GmbH
	 */
	public enum ExpirationMode implements ButtonGroupEnumInterface {
		DURATION("After Duration", "Expiration after a given duration"),
		DATE("On Date", "Expiration on a given date and time");


		private String m_label;

        private String m_desc;


        private ExpirationMode(final String label, final String desc) {
            m_label = label;
            m_desc = desc;
        }

		/**
		 * {@inheritDoc}
		 */
		@Override
		public String getText() {
			return m_label;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public String getActionCommand() {
			return name();
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public String getToolTip() {
			return m_desc;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public boolean isDefault() {
			return DURATION.equals(this);
		}

	}
}
