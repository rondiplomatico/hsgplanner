/* _____________________________________________________________________________
 *
 * Project: PBK AP Hadoop
 * File:    Spieltag.java
 * Version: $Revision$
 * _____________________________________________________________________________
 *
 * Created by:        wirtzd
 * Creation date:     05.09.2018
 * Modified by:       $Author$
 * Modification date: $Date$
 * Description:       See class comment
 * _____________________________________________________________________________
 *
 * Copyright: (C) Daimler AG 2018, all rights reserved
 * _____________________________________________________________________________
 */
package dw.tools.hsg.data;

import java.io.Serializable;
import java.util.Map;

import lombok.Data;
import lombok.ToString;

/**
 * Enthält aufgeschlüsselte Informationen über bestimmte Zeitabschnitte eines
 * Heimspieltages.
 *
 * @author wirtzd
 * @since 05.09.2018
 */
@Data
@ToString
public class Spielzeiten implements Serializable {

	private static final long serialVersionUID = -2000124935112267942L;

	/**
	 * Zeit vom ersten bis zum letzten Anpfiff des Spieltages
	 */
	HSGInterval gesamtZeit;
	/**
	 * Zeit vom ersten bis zum letzten Anpfiff des Spieltages für alles Spiele, bei
	 * denen spieler/aktive Verkaufsdienst machen.
	 */
	HSGInterval spielerVerkaufsZeit;
	/**
	 * Eine Map aus Zeiten für den Elternverkaufsdienst der jeweiligen Teams
	 */
	Map<Team, HSGInterval> elternVerkaufsZeiten;

}
