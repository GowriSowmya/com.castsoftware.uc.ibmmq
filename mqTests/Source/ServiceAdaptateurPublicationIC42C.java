/*
 * ------ tags PVCS - Ne pas modifier svp -----
 *   Composant : %PM%
 *   Revision  : %PR%
 *   DateRev   : %PRT%
 *   Chemin    : %PW%%PM%
 * --------------------------------------------
 *   Historique  :
 *    %PL%
 * --------------------------------------------
 */
package com.francetelecom.artemis.routeur.tools.shell.ic42c;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;

import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.xml.transform.TransformerException;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.francetelecom.artemis.commun.service.Constantes;
import com.francetelecom.artemis.commun.service.util.jdbc.JdbcUtils;
import com.francetelecom.artemis.fwk.util.DateUtils;
import com.francetelecom.artemis.fwk.util.PropertiesUtils;
import com.francetelecom.artemis.fwk.util.StringUtils;
import com.francetelecom.artemis.fwk.util.XmlUtils;
import com.francetelecom.artemis.routeur.service.routagemessage.servicesexternes.ic42c.IC42CConstantes;
import com.ibm.mq.jms.MQQueue;
import com.ibm.mq.jms.MQQueueConnectionFactory;
import com.ibm.mq.jms.MQXAQueueConnectionFactory;

/**
 * <BR><B>HISTORIQUE:</B>
 * <TABLE frame='border' bgcolor=green>
 * <TR><TD>DATE</TD><TD>USER</TD><TD>DETAIL</TD></TR>
 * <TR><TD>02/12/2010</TD><TD>LBA</TD><TD>Ajout accolades autour d'instruction seule</TD></TR>
 * <TR><TD>07/12/2010</TD><TD>DBA</TD><TD>Suppression classe deprecated</TD></TR>
 * <TR><TD>08/12/2010</TD><TD>LBA</TD><TD>Refactoring CAST : passage constante en premier dans assertion equals</TD></TR>
 * <TR><TD>13/12/2010</TD><TD>LBA</TD><TD>Refactoring CAST : utilisation StringUtils pour vérifier les String vides</TD></TR>
 * </TABLE><BR>
 */

/**
 * Classe implémentant la publication d'intention de commande provenant de 42C.
 */
public class ServiceAdaptateurPublicationIC42C {

	private static final Logger LOG = Logger.getLogger(ServiceAdaptateurPublicationIC42C.class);

	/** The connection. */
	private Connection connection;

	/** The anomalies. */
	private StringBuffer anomalies;

	/** The anomalies format champs nd. */
	private StringBuffer anomaliesFormatChampsND;

	/** The rejeu. */
	private boolean rejeu;

	/** The nom fichier. */
	private String nomFichier;

	/** The prefixe. */
	private String prefixe;

	/** The instance se. */
	private String instanceSE;

	/** The no sequence. */
	private int noSequence;

	/** The c date mvt. */
	private Map<String, List<String>> cDateMvt; // liste <idFichier; liste date mvts>

	/** The c validite. */
	private Map<String, String> cValidite; // liste <idFichier; validiteFichier>

	// liste de date sous forme de String au format 'jj/mm/aaaa hh:mm:ss'
	/** The c date publi. */
	private List<String> cDatePubli;

	/** The path du launchLocal. */
	private String pathLaunchLocal;

	/** The launchLocalProperties. */
	private Properties launchLocalProperties = null;

	/**
	 * Methode lancée par le shell IntegrationPubli42C.sh. Détails&nbsp;:
	 * <br/><br/>
	 * Usage de la classe <b>com.francetelecom.artemis.routeur.tools.shell.ic42c.ServiceAdaptateurPublicationIC42C</b> :
	 * <ul>
	 * <li>ServiceAdaptateurPublicationIC42C prefixe nomFichier</li>
	 * </ul>
	 * 
	 * <br/>
	 * <b>Template java de lancement du shell</b><br/>
	 * ${JAVA_HOME}/bin/java \ <br/>
	 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; ${JVM_MEM_ARGS} \ <br/>
	 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; -classpath ${INSTANCE_CLASSPATH_SHELL} \ <br/>
	 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; ${JVM_ARGS} \ <br/>
	 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; com.francetelecom.artemis.routeur.tools.shell.ic42c.ServiceAdaptateurPublicationIC42C \ <br/>
	 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; ${SHELL_ARGS}
	 * 
	 * <br/><br/>
	 * <b>JVM_MEM_ARGS</b> : arguments de la taille memoire de la JVM <i>-Xms512m -Xmx1024m</i> <br/>
	 * <br/>
	 * <b>INSTANCE_CLASSPATH_SHELL</b> : classpath permettant de lancer le shell (présent dans Domaine.sh)<br/>
	 * <br/>
	 * <b>JVM_ARGS</b> : arguments de la jvm <br/>
	 * <ul>
	 * <li>-DUSERORACLE : user oracle </li>
	 * <li>-DPASSWORDORACLE : password oracle </li>
	 * <li>-DORACLE_SERVICE : sid oracle </li>
	 * <li>-DORACLE_HOST : host oracle </li>
	 * <li>-DORACLE_PORT : port oracle </li>
	 * <li>-DConfigFile=artemis.properties </li>
	 * <li>-Dfile.encoding=ISO8859-1 </li>
	 * <li>-Dlog4j.configuration=${WLS_APPLI_SHELL}/properties/log4j_ic42c.xml</li>
	 * </ul>
	 * <br/>
	 * <b>SHELL_ARGS</b> : les arguments propres au shell <br/>
	 * <ol>
	 * <li>prefixe</li>
	 * <li>nomFichier</li>
	 * <li>path launch local</li>
	 * </ol>
	 * <br/>
	 */
	public static void main(String[] args) throws FileNotFoundException, IOException, SQLException {
		if (args.length < 2) {
			throw new RuntimeException("Parametre manquant, Usage : ServiceAdaptateurPublicationIC42C prefixe nomFichier pathLaunchLocal");
		}
		(new ServiceAdaptateurPublicationIC42C()).traiterFichierIC42C(args[0], args[1], args[2]);
	}

	/**
	 * Constructeur.
	 */
	private ServiceAdaptateurPublicationIC42C() {
		LOG.debug("Constructor - User:" + IC42CConstantes.DEFAULT_USER_ID);
		connection = null;
	}

	/**
	 * traiterFichiersIC42C lie chaque ligne d'un fichier 42C, génère un message XML
	 * par ligne, et injecte les messages XML dans une file MQSeries du routeur national.
	 * 
	 * @param a_prefixe the a_prefixe
	 * @param a_nomFichier the a_nom fichier
	 * 
	 * @throws FileNotFoundException the file not found exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws SQLException the SQL exception
	 */
	private void traiterFichierIC42C(String a_prefixe, String a_nomFichier, String a_pathLaunchLocal) throws FileNotFoundException, IOException, SQLException {
		LOG.debug("traiterFichiersIC42C - Debut");
		LOG.debug("traiterFichiersIC42C - Prefixe = " + a_prefixe + "; Nom de fichier = " + a_nomFichier + "; Path LauncLocal = " + a_pathLaunchLocal);
		this.pathLaunchLocal = a_pathLaunchLocal;

		// définition de la liste des messages XML correspondant aux IC 42C
		List<String> l_listeIC42C = new ArrayList<String>();
		BufferedReader l_bufferedReader = null;

		try {
			// ouverture d'une connexion à la base de données
			connection = OutilsPublication.getConnection(connection);

			// initialisation des variables de classe :
			nomFichier = a_nomFichier;
			prefixe = a_prefixe;
			anomalies = new StringBuffer();
			anomaliesFormatChampsND = new StringBuffer();
			rejeu = false;
			noSequence = -1;
			cDatePubli = new ArrayList<String>();

			// récupération du code base 42C
			String l_codeBase42C = getCodeBase42C();
			instanceSE = IC42CConstantes.SYSTEME_EXTERNE_42C + "_" + l_codeBase42C;

			// construction du nom complet du fichier: <path>/<nomFichier>
			String l_path = getFilePath();
			String l_nomCompletFichier = l_path.concat("/").concat(IC42CConstantes.REP_A_TRAITER).concat("/").concat(nomFichier);

			// 1. Récupérer le fichier des intentions de commande 42C
			File l_fichier = new File(l_nomCompletFichier);
			FileReader l_fileReader = new FileReader(l_fichier);
			l_bufferedReader = new BufferedReader(l_fileReader);

			// 2. Préparation aux différents contrôles
			preparerControles();

			// 3. Contrôle de l'entête du fichier
			String l_uneLigne = l_bufferedReader.readLine();
			try {
				if (l_uneLigne != null) {
					traiterEnteteFichier(l_uneLigne);
					l_uneLigne = l_bufferedReader.readLine();
				} else {
					// pas d'entête - fichier vide => RGANO 1 : err n° de sequence
					LOG.error("traiterEnteteFichier - Le numero de sequence n'est pas renseigne!");
					throw new Exception42C(null, Exception42C.ERR_NO_SEQUENCE);
				}
			} catch (Exception42C l_exc) { // RGANO 1
				gererAnomalieEntete(l_exc);
			}

			// 4. Contrôle de la validité des intentions de commande 42C
			int l_nbIC = 0;
			int l_nbICPbND = 0;
			while (l_uneLigne != null) {
				try {
					String l_message = traiterLigneIC42C(l_uneLigne);
					l_listeIC42C.add(l_message);
				} catch (Exception42C l_exc) {
					if (!l_exc.typeErreur.equals(Exception42C.ERR_FORMAT_CHAMPS_ND)) {
						// RG 1 : enregistrer l'anomalie et passer à la ligne suivante
						anomalies.append(l_exc.getLigneEnErreur());
						anomalies.append("|");
						anomalies.append(l_exc.getTypeErreur());
						anomalies.append("\n");
					} else {
						// On enregistre les anomalies du type ERR_FORMAT_CHAMPS_ND dans un StringBuffer différent
						anomaliesFormatChampsND.append(l_exc.getLigneEnErreur());
						anomaliesFormatChampsND.append("|");
						anomaliesFormatChampsND.append(l_exc.getTypeErreur());
						anomaliesFormatChampsND.append("\n");
						l_nbICPbND++;
					}
				}
				l_nbIC++;
				l_uneLigne = l_bufferedReader.readLine();
			}

			// Fin du traitement du fichier des intentions de commande 42C
			gererFinTraitement(l_listeIC42C, l_nbIC, l_nbICPbND);
		} catch (FileNotFoundException l_fileExc) {
			LOG.error("traiterFichiersIC42C - Fichier '" + a_nomFichier + "' non trouve!");
			throw l_fileExc;
		} catch (IOException l_ioExc) {
			LOG.error("traiterFichiersIC42C - Erreur lors de la lecture du fichier '" + nomFichier + "' ou son fichier d'erreur!");
			throw l_ioExc;
		} catch (SQLException l_sqlExc) {
			LOG.error("traiterFichiersIC42C - Pb d'acces a la base lors du traitement du fichier '" + nomFichier + "'!");
			throw l_sqlExc;
		} finally {
			// fermeture du flux d'E/S
			if (l_bufferedReader != null) {
				l_bufferedReader.close();
			}
			// fermeture de la connexion à la base de données
			JdbcUtils.closeConnection(connection);
		}
		LOG.debug("traiterFichiersIC42C - Fin");
	} // fin de traiterFichierIC42C

	/**
	 * getCodeBase42C récupère le code base 42C contenu dans le nom du fichier.
	 * 
	 * @return the code base42 c
	 */
	private String getCodeBase42C() {
		LOG.debug("getCodeBase42C - Debut");
		LOG.debug("getCodeBase42C - Nom du fichier : " + nomFichier);

		String l_codeBase42C = nomFichier.substring(IC42CConstantes.INDEX_CODE_BASE, IC42CConstantes.INDEX_CODE_BASE + IC42CConstantes.TAILLE_CODE_BASE);

		LOG.debug("getCodeBase42C - Fin : code base 42c = " + l_codeBase42C);
		return l_codeBase42C;
	}

	/**
	 * traiterEnteteFichier traite l'entête du fichier d'IC 42C.
	 * 
	 * @param a_entete the a_entete
	 * 
	 * @throws SQLException the SQL exception
	 */
	private void traiterEnteteFichier(String a_entete) throws SQLException {
		LOG.debug("traiterEnteteFichier - Debut");
		LOG.debug("traiterEnteteFichier - Instance Systeme Externe : " + instanceSE + "; Entete = " + a_entete);

		// 1. Vérification du nombre de séparateurs
		String[] l_enteteTokenizer = a_entete.split(IC42CConstantes.SEPARATEUR, -1);
		int l_nbChamps = l_enteteTokenizer.length;
		LOG.debug("traiterEnteteFichier - nb champs = " + l_nbChamps);
		if (l_nbChamps > 2) {
			// RGANO 1 - Err_Separateur_Champs
			LOG.error("traiterEnteteFichier - Le format de l'entete n'est pas conforme! (nb champs = " + l_nbChamps + ")");
			throw new Exception42C(a_entete, Exception42C.ERR_SEPARATEUR_CHAMPS);
		}

		// récupération du numéro de séquence
		String l_stNoSequenceFichier = l_enteteTokenizer[0];

		// récupération du "Rejeu"
		if (l_nbChamps == 2) {
			String l_rejeu = l_enteteTokenizer[1];
			rejeu = l_rejeu.equals(IC42CConstantes.ENTETE_REJEU);
			LOG.debug("traiterEnteteFichier - positionnement du rejeu : " + rejeu);
		}

		// 2. Vérification de la présence, du format et de la taille du numéro de séquence
		if ((l_stNoSequenceFichier == null) || (l_stNoSequenceFichier.length() == 0) || (l_stNoSequenceFichier.length() != 5)) {
			// numéro de séquence absent ou pas sur 5 caractères :
			// RGANO 1 - Err_NoSequence
			LOG.error("traiterEnteteFichier - Le numero de sequence (" + l_stNoSequenceFichier + ") n'est pas renseigne ou il n'est pas sur 5 caracteres!");
			throw new Exception42C(a_entete, Exception42C.ERR_NO_SEQUENCE);
		}
		try {
			noSequence = Integer.parseInt(l_stNoSequenceFichier);
		} catch (NumberFormatException l_numExc) {
			// numéro de séquence contient 1 ou plusieurs caracteres alpha-numeriques
			// RGANO 1 - Err_NoSequence
			LOG.error("traiterEnteteFichier - Le numero de sequence ne doit pas contenir de caracteres alpha-numeriques!");
			throw new Exception42C(a_entete, Exception42C.ERR_NO_SEQUENCE);
		}

		// 3. Vérification du numéro de séquence
		int l_noSequenceDB = OutilsPublication.getNoSequence(connection, instanceSE);
		if (l_noSequenceDB == 0) {
			// RGANO 1 - Err_DonneesReference
			LOG.error("traiterEnteteFichier - Erreur au niveau des donnees de reference!");
			throw new Exception42C(a_entete, Exception42C.ERR_DONNEES_REFERENCE);
		} else if (l_noSequenceDB != noSequence) {
			// RGANO 1 - Err_NoSequence
			LOG.error("traiterEnteteFichier - Le numero de sequence non attendu!");
			throw new Exception42C(a_entete, Exception42C.ERR_NO_SEQUENCE);
		}

		LOG.debug("traiterEnteteFichier - Fin");
	} // fin de traiterEnteteFichier

	/**
	 * traiterLigneIC42C traite une Intention de Commande :
	 * 1/ préparation préalable aux contrôles de dates
	 * 2/ vérification du nombre de séparateurs
	 * 3/ vérification de la présence, le format et la taille des champs
	 * 3/ vérification des dates de publication
	 * 3/ récupèration du ND
	 * 4/ génèration du message XML.
	 * 
	 * @param a_ic the a_ic
	 * 
	 * @return le message XML
	 */
	private String traiterLigneIC42C(String a_ic) {
		LOG.debug("traiterLigneIC42C - Debut");
		LOG.debug("traiterLigneIC42C - Traitement de l'intention de commande : " + a_ic);
		String l_message = null;

		// 1. Préparation préalable aux contrôles de dates
		Date l_datePremierMvt = new Date();
		if (!rejeu) {
			// faire le test sur les dates
			Iterator<String> l_iteratorIdFichier = cDateMvt.keySet().iterator();
			while (l_iteratorIdFichier.hasNext()) {
				// pour chaque fichier traité
				String l_idFichier = l_iteratorIdFichier.next();
				LOG.debug("traiterLigneIC42C - l_idFichier = " + l_idFichier);
				// récupéré la liste de ses date de mouvements
				List<String> l_listeDateMvt = cDateMvt.get(l_idFichier);

				// valorisation de la date du premier mouvement sur cette base 42c
				// en base les dates sont des String au format 'jj/mm/aaaa hh:mm:ss'
				try {
					for (String l_stDateMvt : l_listeDateMvt) {
						Date l_dateMvt = DateUtils.parseDate(l_stDateMvt, IC42CConstantes.FORMAT_DATE_HEURE);
						if (l_dateMvt.compareTo(l_datePremierMvt) < 0) {
							l_datePremierMvt = l_dateMvt;
						}
					}
				} catch (ParseException l_exc) {
					LOG.error("ajouterDatePubli - Erreur de format au niveau de la date de premier mouvement : " + l_exc.getMessage());
					throw new Exception42C(a_ic, Exception42C.ERR_DATE_PUBLICATION);
				}
			}
		}
		LOG.debug("traiterLigneIC42C - datePremierMvt = " + l_datePremierMvt);

		// 2. Vérification du nombre de séparateurs
		String[] l_icTokenizer = a_ic.split(IC42CConstantes.SEPARATEUR, -1);
		int l_nbChamps = l_icTokenizer.length;
		LOG.debug("traiterLigneIC42C - nb champs = " + l_nbChamps);
		if (l_nbChamps != 16) {
			// executer RG 1
			LOG.error("traiterLigneIC42C - Le format du message d'IC 42C n'est pas conforme! (nb champs = " + l_nbChamps + ")");
			throw new Exception42C(a_ic, Exception42C.ERR_SEPARATEUR_CHAMPS);
		}

		// 3. Vérification de la présence, du format et de la taille des champs,
		// vérification des dates de publication et récupération du ND
		String l_nd = validerChamps(l_icTokenizer, a_ic, l_datePremierMvt);
		LOG.debug("traiterLigneIC42C - ND = " + l_nd);
		if (l_nd != null) {
			// 4. Génération du message XML
			l_message = genererMessage(l_nd, a_ic);
		}

		LOG.debug("traiterLigneIC42C - Fin : message genere = " + l_message);
		return l_message;
	} // fin de traiterLigneIC42C

	/**
	 * preparerControles récupére pour le même code base 42c : la liste des fichiers
	 * pour chaque fichier sa liste de date de mvts et sa validité.
	 * 
	 * @throws SQLException the SQL exception
	 */
	private void preparerControles() throws SQLException {
		LOG.debug("preparerControles - Debut");
		cDateMvt = new HashMap<String, List<String>>(); // liste <idFichier; liste date mvts>
		cValidite = new HashMap<String, String>(); // liste <idFichier; validiteFichier>

		// récupérer pour le même code base 42c : la liste des fichiers
		// pour chaque fichier sa liste de date de mvts et sa validité
		OutilsPublication.getInfosInstanceSE(connection, instanceSE, cDateMvt, cValidite);

		LOG.debug("preparerControles - Fin");
	} // fin de preparerControles

	/**
	 * RGANO 1 :
	 * gererAnomalieEntete gere es anomalies générées lors du contrôle de l'entête.
	 * 
	 * @param a_exception the a_exception
	 * 
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws SQLException the SQL exception
	 */
	private void gererAnomalieEntete(Exception42C a_exception) throws IOException, SQLException {
		LOG.debug("gererAnomalieEntete - Debut");

		// ajouter l'exception aux anomalies
		anomalies.append(a_exception.getLigneEnErreur());
		anomalies.append("|");
		anomalies.append(a_exception.getTypeErreur());
		anomalies.append("\n");

		LOG.error("gererAnomalieEntete - Interruption Traitement Publication 42C : Le traitement du fichier '" + nomFichier + "' a genere des anomalies!");
		creerFichierErreur();
		insererDonnees(OutilsConstantes.VALIDITE_NOK);
		terminerTraitementEnErreur();

		LOG.debug("gererAnomalieEntete - Fin");
	} // fin de gererAnomalieEntete

	/**
	 * gererFinTraitement execute la fin de traitement sur le fichier des intentions de
	 * commande 42C.
	 * 
	 * @param a_listeIC42C the a_liste i c42 c
	 * @param a_nbIC the a_nb ic
	 * @param a_nbICPbND the a_nb ic pb nd
	 * 
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws SQLException the SQL exception
	 */
	private void gererFinTraitement(List<String> a_listeIC42C, int a_nbIC, int a_nbICPbND) throws IOException, SQLException {
		LOG.debug("gererFinTraitement - Debut");
		if (anomalies.length() > 0) {
			// RGANO 2
			// Le traitement a généré des anomalies
			LOG.error("gererFinTraitement - Interruption Traitement Publication 42C : Le traitement du fichier '" + nomFichier + "' a genere des anomalies!");
			creerFichierErreur();
			insererDonnees(OutilsConstantes.VALIDITE_NOK, a_nbIC, a_nbICPbND);
			terminerTraitementEnErreur();
		} else {
			if (anomalies.length() == 0 && anomaliesFormatChampsND.length() > 0) {
				// RGANO 3
				// Seules des anomalies Err_Format_Champs_ND ont été détectées
				LOG.info("gererFinTraitement - Probleme Formatage d'un ND : Le traitement du fichier '" + nomFichier + "' a genere des anomalies" + " du type Err_Format_Champs_ND");
				creerFichierErreur();
				updateNoSequence();
				insererDonnees(OutilsConstantes.VALIDITE_PARTIELLE, a_nbIC, a_nbICPbND);
			}
			if (anomalies.length() == 0 && anomaliesFormatChampsND.length() == 0) {
				// Le traitement du fichier n'a pas généré d'anomalies
				LOG.info("gererFinTraitement - Le traitement du fichier '" + nomFichier + "' s'est bien deroule!");
				updateNoSequence();
				insererDonnees(OutilsConstantes.VALIDITE_OK, a_nbIC, a_nbICPbND);
			}
			// Envoyer les messages XML dans la file destination
			for (String message : a_listeIC42C) {
				envoyerMessageXML(message);
			}
		}

		LOG.debug("gererFinTraitement - Fin");
	} // fin de gererFinTraitement

	/**
	 * RG 2 :
	 * getND extrait le ND d'une IC 42C.
	 * 
	 * @param a_ancienND the a_ancien nd
	 * 
	 * @return the ND
	 */
	private String getND(String a_ancienND) {
		LOG.debug("getND - Debut");
		String l_nd = null;

		try {
			// Ajouter, en tête de ce ND, la valeur du préfixe
			l_nd = prefixe.concat(a_ancienND);

			// Vérifier le format du ND
			Integer.parseInt(l_nd);
			if (l_nd.length() != 10) {
				// trop ou pas assez de chiffre dans le ND
				l_nd = null;
			}
		} catch (NoSuchElementException l_exc) {
			l_nd = null;
		} catch (NumberFormatException l_numExc) {
			l_nd = null;
		}

		LOG.debug("getND - Fin");
		return l_nd;
	}

	/**
	 * genererMessage génere une message XML à partir des infos passées en paramètre.
	 * 
	 * @param a_nd the a_nd
	 * @param a_ic the a_ic
	 * 
	 * @return the string
	 */
	private String genererMessage(String a_nd, String a_ic) {
		LOG.debug("genererMessage - Debut");

		Document document = XmlUtils.getDocument();

		// Génération de la racine du message XML
		Element l_root = document.createElement(IC42CConstantes.TAG_ROOT);

		// Génération du noeud Intention de Commande
		Element l_noeudIC = document.createElement(IC42CConstantes.TAG_IC);
		// Ajout de l'attribut ND
		l_noeudIC.setAttribute(IC42CConstantes.ATT_ND, a_nd);
		// Ajout de l'attribut Message
		l_noeudIC.setAttribute(IC42CConstantes.ATT_MESSAGE, a_ic);
		// Ajout de l'attribut Prefixe
		l_noeudIC.setAttribute(IC42CConstantes.ATT_PREFIXE, prefixe);

		// Ajout du noeud Intention de Commande à la racine
		l_root.appendChild(l_noeudIC);

		// Ajout de la racine au document
		document.appendChild(l_root);

		LOG.debug("genererMessage - Fin");
		String result;
		try {
			result = XmlUtils.xmlToString(document);
		} catch (TransformerException te) {
			LOG.error("genererMessage - Exception dans la création du document xml ", te);
			throw new RuntimeException("Exception dans la création du document xml ", te);
		}
		return result;
	}

	/**
	 * envoyerMessageXML poste le message passé en entrée dans la file
	 * 'messageManager'.
	 * 
	 * @param message the message
	 * 
	 * @throws JMSException
	 * @throws javax.naming.NamingException
	 */
	private void envoyerMessageXML(String message) {
		LOG.debug("envoyerMessageXML - Debut");
		try {
			sendMessage(message);
		} catch (JMSException e) {
			LOG.error("envoyerMessageXML - Exception dans l'envoi du message xml ", e);
		}
		LOG.debug("envoyerMessageXML - Fin");
	}

	/**
	 * Envoie un message JMS de type Text
	 * 
	 * @param queueFactory the queue factory
	 * @param queueName the queue name
	 * @param msg the msg
	 * @throws JMSException
	 * 
	 * @throws javax.naming.NamingException the naming exception
	 * @throws JMSException the JMS exception
	 */
	private void sendMessage(String message) throws JMSException {
		QueueSession queueSession = null;
		try {
			QueueConnectionFactory queueConnectionFactory = new MQXAQueueConnectionFactory();
			((MQQueueConnectionFactory) queueConnectionFactory).setQueueManager(getQueueFactoryName());
			QueueConnection queueConnection = queueConnectionFactory.createQueueConnection();
			queueConnection.start();
			queueSession = queueConnection.createQueueSession(true, Session.AUTO_ACKNOWLEDGE);
			Queue queue = new MQQueue(getQueueName());
			QueueSender queueSender = queueSession.createSender(queue);
			TextMessage textMessage = queueSession.createTextMessage();
			textMessage.setText(message);
			queueSender.send(textMessage);
			queueSession.commit();
		} finally {
			try {
				if (queueSession != null) {
					queueSession.close();
				}
			} catch (JMSException jmse) {
			}
		}
	}

	/**
	 * getQueueName retourne le nom de la file de message.
	 * Ce nom est défini dans artemis.properties
	 * 
	 * @return the queue name
	 */
	private String getQueueName() {
		LOG.debug("getQueueName - Debut");
		String l_queueName = PropertiesUtils.getConfig(Constantes.ARTEMIS_PROP_FILE).getString(IC42CConstantes.PROPERTY_QUEUE_NAME);
		LOG.debug("getQueueName - queueName = " + l_queueName);
		String l_mqQueueName = getLaunchLocalValue("queue", l_queueName);
		LOG.debug("getQueueName - mq queueName = " + l_mqQueueName);
		LOG.debug("getQueueName - Fin");
		return l_mqQueueName;
	}

	/**
	 * getQueueFactoryName retourne le nom de la 'factory' de files.
	 * Ce nom est défini dans artemis.properties.
	 * 
	 * @return the queue factory name
	 */
	private String getQueueFactoryName() {
		LOG.debug("getQueueFactoryName - Debut");
		String l_queueFactoryName = PropertiesUtils.getConfig(Constantes.ARTEMIS_PROP_FILE).getString(IC42CConstantes.PROPERTY_FACTORY_QUEUE_NAME);
		LOG.debug("getQueueFactoryName - queueFactoryName = " + l_queueFactoryName);
		String l_mqQueueFactoryName = getLaunchLocalValue("factory", l_queueFactoryName);
		int indexVirgule = l_mqQueueFactoryName.indexOf(',');
		if (indexVirgule != -1) {
			l_mqQueueFactoryName = l_mqQueueFactoryName.substring(0, indexVirgule);
		}
		LOG.debug("getQueueFactoryName - mq queueFactoryName = " + l_mqQueueFactoryName);
		LOG.debug("getQueueFactoryName - Fin");
		return l_mqQueueFactoryName;
	}

	/**
	 * Récupère la valeur dans le launch local correspondante à la clé passée en paramètre
	 * 
	 * @param cle
	 * @return valeur
	 */
	private String getLaunchLocalValue(String prefixCle, String cle) {
		Properties properties = getLaunchLocalAsProperties();
		return properties.getProperty(prefixCle + "." + cle);
	}

	/**
	 * Récupération du launchlocal.dat sous forme de properties
	 * 
	 * @return Properties
	 */
	private Properties getLaunchLocalAsProperties() {
		if (launchLocalProperties == null) {
			String filePath = pathLaunchLocal + "/launchlocal.dat";
			launchLocalProperties = new Properties();
			try {
				launchLocalProperties.load(new FileInputStream(filePath));
			} catch (Exception e) {
				LOG.error("getLaunchLocalAsProperties - Impossible d'ouvrir le fichier launchlocal.dat : " + filePath, e);
				throw new Exception42C("launchlocal.dat non trouvé");
			}
		}
		return launchLocalProperties;
	}

	/**
	 * getFilePath retourne le chemin d'accès aux fichiers d'IC42C
	 * Ce chemin est défini dans artemis.properties.
	 * 
	 * @return the file path
	 */
	private String getFilePath() {
		LOG.debug("getFilePath - Debut");
		String l_path = PropertiesUtils.getConfig(Constantes.ARTEMIS_PROP_FILE).getString(IC42CConstantes.PROPERTY_PATH);
		LOG.debug("getFilePath - path = " + l_path);
		LOG.debug("getFilePath - Fin");
		return l_path;
	}

	/**
	 * validerChamps effectue la vérification de la présence, du format et de la
	 * taille des champs de l'IC 42C, la vérification des dates de publication et
	 * la récupération du ND.
	 * 
	 * @param a_msgTokenizer the a_msg tokenizer
	 * @param a_ic the a_ic
	 * @param a_datePremierMvt the a_date premier mvt
	 * 
	 * @return the string
	 */
	private String validerChamps(String[] a_msgTokenizer, String a_ic, Date a_datePremierMvt) {
		LOG.debug("validerChamps - Debut");

		// 1. Vérification de la présence, du format et de la taille des champs
		// vérification du champ Type de Commande
		String l_typeCommande = a_msgTokenizer[0];
		validerFormatChamp(l_typeCommande, IC42CConstantes.TAILLE_MAX_TYPE_CDE, true, "Type de commande", a_ic);

		// vérification du champ Date de Publication
		// dans l'IC 42C, les dates sont au format 'jj/mm/aaaa'
		String l_stDatePublication = a_msgTokenizer[1];
		validerFormatChamp(l_stDatePublication, IC42CConstantes.TAILLE_MAX_DATE, IC42CConstantes.TAILLE_MAX_DATE, true, "Date de publication", a_ic);

		// vérification du champ Date d'Evénement
		String l_dateEvt = a_msgTokenizer[2];
		validerFormatChamp(l_dateEvt, IC42CConstantes.TAILLE_MAX_DATE, IC42CConstantes.TAILLE_MAX_DATE, true, "Date d'evenement", a_ic);

		// vérification du champ Ancien ND
		String l_ancienND = a_msgTokenizer[3];
		validerFormatChamp(l_ancienND, IC42CConstantes.TAILLE_MAX_ND, true, "Ancien ND", a_ic);

		// vérification du champ Nouveau ND
		String l_nouveauND = a_msgTokenizer[4];
		validerFormatChamp(l_nouveauND, IC42CConstantes.TAILLE_MAX_ND, false, "Nouveau ND", a_ic);

		// vérification du champ Ancien Nom
		String l_ancienNom = a_msgTokenizer[5];
		validerFormatChamp(l_ancienNom, IC42CConstantes.TAILLE_MAX_NOM, true, "Ancien nom", a_ic);

		// vérification du champ Nouveau Nom
		String l_nouveauNom = a_msgTokenizer[6];
		validerFormatChamp(l_nouveauNom, IC42CConstantes.TAILLE_MAX_NOM, false, "Nouveau nom", a_ic);

		// vérification du champ Offre
		String l_offre = a_msgTokenizer[7];
		validerFormatChamp(l_offre, IC42CConstantes.TAILLE_MAX_OFFRE, IC42CConstantes.TAILLE_MAX_OFFRE, true, "Offre", a_ic);

		// vérification du champ Prestation
		String l_prestation = a_msgTokenizer[8];
		validerFormatChamp(l_prestation, IC42CConstantes.TAILLE_MAX_PRESTATION, IC42CConstantes.TAILLE_MAX_PRESTATION, false, "Prestation", a_ic);

		// vérification du champ Code Provider
		String l_codeProvider = a_msgTokenizer[9];
		validerFormatChamp(l_codeProvider, IC42CConstantes.TAILLE_MAX_CODE_PROVIDER, IC42CConstantes.TAILLE_MAX_CODE_PROVIDER, false, "Code provider", a_ic);

		// vérification du champ Renvoi Répartiteur
		String l_renvoiRepartiteur = a_msgTokenizer[10];
		validerFormatChamp(l_renvoiRepartiteur, IC42CConstantes.TAILLE_MAX_RENVOI_REPART, IC42CConstantes.TAILLE_MAX_RENVOI_REPART, false, "Renvoi repartiteur", a_ic);

		// vérification du champ Motif Résiliation
		String l_motifResiliation = a_msgTokenizer[11];
		validerFormatChamp(l_motifResiliation, IC42CConstantes.TAILLE_MAX_MOTIF_RESIL, IC42CConstantes.TAILLE_MAX_MOTIF_RESIL, false, "Motif résiliation", a_ic);

		// vérification du champ Segment de Marché
		String l_segmentMarche = a_msgTokenizer[12];
		validerFormatChamp(l_segmentMarche, IC42CConstantes.TAILLE_MAX_SEGMENT_MARCHE, IC42CConstantes.TAILLE_MAX_SEGMENT_MARCHE, true, "Segment de marche", a_ic);

		// vérification du champ EDS Installation
		String l_edsInstallation = a_msgTokenizer[13];
		validerFormatChamp(l_edsInstallation, IC42CConstantes.TAILLE_MAX_EDS_INSTALL, IC42CConstantes.TAILLE_MAX_EDS_INSTALL, false, "EDS installation", a_ic);

		// vérification du champ Centre Rattachement
		String l_centreRattachement = a_msgTokenizer[14];
		validerFormatChamp(l_centreRattachement, IC42CConstantes.TAILLE_MAX_CENTRE_RATTACH, IC42CConstantes.TAILLE_MAX_CENTRE_RATTACH, true, "Centre rattachement", a_ic);

		// vérification du champ Opérateur
		String l_operateur = a_msgTokenizer[15];
		validerFormatChamp(l_operateur, IC42CConstantes.TAILLE_MAX_OPERATEUR, IC42CConstantes.TAILLE_MAX_OPERATEUR, false, "Operateur", a_ic);

		// 2. Vérification des dates de publication
		if (!rejeu) {
			controlerDatePublication(l_stDatePublication, a_datePremierMvt, a_ic);
		} // fin if (! rejeu)

		// ajouter la date de publication dans la collection cDatePubli
		ajouterDatePubli(l_stDatePublication, a_ic);

		// 3. Récupération du ND
		String l_ND = getND(l_ancienND);
		if (l_ND == null) {
			// Erreur de format sur le ND
			LOG.error("validerChamps - Erreur au niveau du format du ND");
			throw new Exception42C(a_ic, Exception42C.ERR_EXTRACTION_ND);
		}

		LOG.debug("validerChamps - Fin");
		return l_ND;
	} // fin de validerChamps

	/**
	 * validerFormatChamp valide si un champ d'IC 42C est au bon format : la
	 * taille max et la présence du champs. Pas de vérification sur la taille exacte!
	 * 
	 * @param a_champ the a_champ
	 * @param a_tailleMax the a_taille max
	 * @param a_obligatoire the a_obligatoire
	 * @param a_nomChamp the a_nom champ
	 * @param a_ic the a_ic
	 */
	private void validerFormatChamp(String a_champ, int a_tailleMax, boolean a_obligatoire, String a_nomChamp, String a_ic) {
		validerFormatChamp(a_champ, a_tailleMax, -1, a_obligatoire, a_nomChamp, a_ic);
	}

	/**
	 * validerFormatChamp valide si un champ d'IC 42C est au bon format : vérifie
	 * la taille taille exacte (si celle-ci est précisée), la taille max, et la
	 * présence du champs.
	 * 
	 * @param a_champ the a_champ
	 * @param a_tailleMax the a_taille max
	 * @param a_tailleExact si pas de taille exacte précisée : par défaut mettre -1
	 * @param a_obligatoire the a_obligatoire
	 * @param a_nomChamp the a_nom champ
	 * @param a_ic the a_ic
	 */
	private void validerFormatChamp(String a_champ, int a_tailleMax, int a_tailleExact, boolean a_obligatoire, String a_nomChamp, String a_ic) {
		LOG.debug("validerFormatChamp - Debut : demande de validation du champ " + a_nomChamp + " dont la valeur est '" + a_champ + "'");

		int l_tailleChamp = a_champ.length();

		// Vérification de l'existence du champ, si celui-ci est obligatoire
		if (a_obligatoire && (l_tailleChamp == 0)) {
			LOG.error("validerFormatChamp - Le champ " + a_nomChamp + " n'est pas renseigne!");
			throw new Exception42C(a_ic, Exception42C.ERR_CHAMPS_OBLIGATOIRE_ABSENT);
		}

		// Vérification de la taille du champ
		if ((a_tailleExact != -1) && (l_tailleChamp != a_tailleExact) && (l_tailleChamp > 0)) {
			LOG.error("validerFormatChamp - Le champ " + a_nomChamp + " n'est pas de la bonne taille (" + a_tailleExact + ")!");
			throw new Exception42C(a_ic, Exception42C.ERR_TAILLE_CHAMPS);
		}
		if (l_tailleChamp > a_tailleMax) {
			LOG.error("validerFormatChamp - Le champ " + a_nomChamp + " est trop grand!");
			throw new Exception42C(a_ic, Exception42C.ERR_TAILLE_CHAMPS);
		}

		// Traitement spécifique pour la validation des ND
		if ("Ancien ND".equals(a_nomChamp) || "Nouveau ND".equals(a_nomChamp)) {
			try {
				// Vérifier le format du ND
				if (StringUtils.isNotEmpty(a_champ)) {
					Integer.parseInt(a_champ);
				}
			} catch (NumberFormatException l_numExc) {
				LOG.info("validerFormatChamp - Le champ " + a_nomChamp + " '" + a_champ + "'" + " n'est pas numerique!");
				throw new Exception42C(a_ic, Exception42C.ERR_FORMAT_CHAMPS_ND);
			}
		}

		LOG.debug("validerFormatChamp - Fin");
	} // fin de validerFormatChamp

	/**
	 * Controler date publication.
	 * 
	 * @param a_stDatePublication au format 'jj/mm/aaaa'
	 * @param a_datePremierMvt the a_date premier mvt
	 * @param a_ic the a_ic
	 */
	private void controlerDatePublication(String a_stDatePublication, Date a_datePremierMvt, String a_ic) {
		LOG.debug("controlerDatePublication - Debut");
		LOG.debug("controlerDatePublication - date de publication = " + a_stDatePublication);

		String l_validiteLigne = OutilsConstantes.VALIDITE_OK;

		// récupération de la date de publication sous forme de Date et formattage
		// de la date du jour et de la date du premier mouvement au format : 'jj/mm/aaaa'
		try {
			// date de publication
			Date l_datePublication = DateUtils.parseDate(a_stDatePublication, IC42CConstantes.FORMAT_DATE);

			// date du jour
			Date l_dateJour = DateUtils.getNowDate();

			if (l_datePublication.compareTo(l_dateJour) > 0) {
				LOG.error("controlerDatePublication - Erreur la date de publication est superieure a la date du jour");
				throw new Exception42C(a_ic, Exception42C.ERR_DATE_PUBLICATION);
			}

			if (l_datePublication.compareTo(a_datePremierMvt) < 0) {
				LOG.error("controlerDatePublication - Erreur la date de publication est inferieure a la date de premier mouvement (" + a_datePremierMvt + ")");
				throw new Exception42C(a_ic, Exception42C.ERR_DATE_PUBLICATION);
			}

			Iterator<String> l_itListeIdFichiers = cDateMvt.keySet().iterator();
			while (l_itListeIdFichiers.hasNext()) {
				String l_idFichier = l_itListeIdFichiers.next();
				List<String> listeDateMvt = cDateMvt.get(l_idFichier);
				for (String stDateMvt : listeDateMvt) {
					Date l_dateMvt = DateUtils.parseDate(stDateMvt, IC42CConstantes.FORMAT_DATE_HEURE);
					if (l_datePublication.compareTo(l_dateMvt) == 0) {
						String l_validiteFichier = cValidite.get(l_idFichier);
						if (l_validiteFichier.equals(OutilsConstantes.VALIDITE_OK) || l_validiteFichier.equals(OutilsConstantes.VALIDITE_PARTIELLE)) {
							l_validiteLigne = OutilsConstantes.VALIDITE_NOK;
						}
					}
				}
			}
		} catch (ParseException l_exc) {
			LOG.error("controlerDatePublication - Erreur de format au niveau des date de publication, date du jour, ou date de mouvements : " + l_exc.getMessage());
			throw new Exception42C(a_ic, Exception42C.ERR_DATE_PUBLICATION);
		}

		if (l_validiteLigne.equals(OutilsConstantes.VALIDITE_NOK)) {
			LOG.error("controlerDatePublication - Erreur : la validite de la ligne = NOK");
			throw new Exception42C(a_ic, Exception42C.ERR_DATE_PUBLICATION);
		}

		LOG.debug("controlerDatePublication - Fin");
	} // fin de controlerDatePublication

	/**
	 * ajouterDatePubli : si la valeur de la date de publication n'existe pas
	 * dans la collection des dates de publications de ce fichier, alors l'ajouter.
	 * 
	 * @param a_stDatePublication au format 'jj/mm/aaaa'
	 * @param a_ic the a_ic
	 */
	private void ajouterDatePubli(String a_stDatePublication, String a_ic) {
		LOG.debug("ajouterDatePubli - Debut");

		// transformation du format de la date de publication récupérée dans l'IC 42C
		// (jj/mm/aaaa) en format pour la base (jj/mm/aaaa hh:mm:ss)
		// en effet dans l'IC 42C, les dates sont au format 'jj/mm/aaaa' et
		// en base les dates sont des String au format 'jj/mm/aaaa hh:mm:ss'
		String l_stTmpDatePub = a_stDatePublication;
		try {
			Date l_datePub = DateUtils.parseDate(a_stDatePublication, IC42CConstantes.FORMAT_DATE);
			l_stTmpDatePub = DateUtils.format(l_datePub, IC42CConstantes.FORMAT_DATE_HEURE);
		} catch (ParseException l_exc) {
			LOG.error("ajouterDatePubli - Erreur de format au niveau de la date de publication : " + l_exc.getMessage());
			throw new Exception42C(a_ic, Exception42C.ERR_DATE_PUBLICATION);
		}

		LOG.debug("ajouterDatePubli - date de publication = " + a_stDatePublication + ";" + l_stTmpDatePub);

		boolean l_trouve = false;
		for (String l_dateTmp : cDatePubli) {
			if (l_dateTmp.equals(l_stTmpDatePub)) {
				l_trouve = true;
				break;
			}
		}
		if (!l_trouve) {
			// la date de publication n'existe pas dans la collection des dates de
			// publications de ce fichier
			cDatePubli.add(l_stTmpDatePub);
		}

		LOG.debug("ajouterDatePubli - Fin");
	}

	/**
	 * creerFichierErreur genère le fichier de rejet.
	 * 
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	private void creerFichierErreur() throws IOException {
		LOG.debug("creerFichierErreur - Debut");

		try {
			// Construction du nom complet du fichier: <path>/<nomFichier>
			String l_path = getFilePath();
			String l_nomFichierErr = nomFichier.concat(IC42CConstantes.SUFFIXE_ERREUR);
			String l_nomCompletFichier = l_path.concat("/").concat(IC42CConstantes.REP_TRT_ERREUR).concat("/").concat(l_nomFichierErr);

			// Création d'un fichier contenant les IC en erreur
			File l_fichier = new File(l_nomCompletFichier);
			FileWriter l_fichierErr = new FileWriter(l_fichier);

			// Copier les lignes d'IC en erreur dans le fichier
			l_fichierErr.write(anomalies.toString());
			l_fichierErr.write(anomaliesFormatChampsND.toString());

			// Fermer le fichier
			l_fichierErr.close();
		} catch (IOException l_ioExc) {
			LOG.error("creerFichierErreur - Erreur lors du traitement des anomalies du fichier " + nomFichier);
			throw l_ioExc;
		}

		LOG.debug("creerFichierErreur - Fin");
	} // fin de creerFichierErreur

	/**
	 * insererDonnees insert en base :
	 * \n * un nouvel enregistrement dans JournalEnvoisCFT avec idFichier = noSequence
	 * concaténé à la date actuelle
	 * \n * des enregistrements dans la table CaractéristiquesEnvois : le numero de
	 * sequence, la date de traitement et la validité du fichier.
	 * 
	 * @param a_validiteFichier the a_validite fichier
	 * 
	 * @throws SQLException the SQL exception
	 */
	private void insererDonnees(String a_validiteFichier) throws SQLException {
		insererDonnees(a_validiteFichier, -1, 0);
	}

	/**
	 * insererDonnees insert en base :
	 * \n * un nouvel enregistrement dans JournalEnvoisCFT avec idFichier = noSequence
	 * concaténé à la date actuelle
	 * \n * des enregistrements dans la table CaractéristiquesEnvois : le numero de
	 * sequence, la date de traitement, la validité du fichier, et éventuellement le nb
	 * d'IC et les dates de publication de ces IC. Rmq : le nb d'IC et les dates
	 * de publications ne sont inséré uniquement si le paramètre en entrée a_nbIC
	 * est différent de -1.
	 * 
	 * @param a_validiteFichier the a_validite fichier
	 * @param a_nbIC correspond au nb d'IC contenues dans le fichier; valoriser à
	 * -1 pour ne pas inserer le nb d'IC et les dates de publications dans la table
	 * CaractéristiquesEnvois
	 * @param a_nbICPbND the a_nb ic pb nd
	 * 
	 * @throws SQLException the SQL exception
	 */
	private void insererDonnees(String a_validiteFichier, int a_nbIC, int a_nbICPbND) throws SQLException {
		LOG.debug("insererDonnees - Debut");

		// 1. Insertion dans la table JournalEnvoisCFT
		// récupération de la date actuelle à la seconde près
		String l_stDateJour = DateUtils.format(new Date(), IC42CConstantes.FORMAT_DATE_HEURE);

		// récupération du numéro de séquence
		String l_stNoSequence = Integer.toString(noSequence);
		String l_idFichier = l_stNoSequence + "_" + l_stDateJour;
		OutilsPublication.insertJournalEnvoisCFT(connection, instanceSE, l_idFichier);

		// 2. Insertions dans la table CaracteristiquesEnvois
		// insertion du numero de sequence
		OutilsPublication.insertCaracteristiqueEnvois(connection, l_idFichier, OutilsConstantes.CLE_NUMERO_SEQUENCE, l_stNoSequence);
		// insertion de la date de traitement
		OutilsPublication.insertCaracteristiqueEnvois(connection, l_idFichier, OutilsConstantes.CLE_DATE_TRAITEMENT, l_stDateJour);
		// insertion de la validite du fichier
		OutilsPublication.insertCaracteristiqueEnvois(connection, l_idFichier, OutilsConstantes.CLE_VALIDITE_FICHIER, a_validiteFichier);

		if (a_nbIC != -1) {
			// insertion du nb d'IC contenues dans le fichier
			OutilsPublication.insertCaracteristiqueEnvois(connection, l_idFichier, OutilsConstantes.CLE_NOMBRE_IC, Integer.toString(a_nbIC));

			// insertion du nb d'IC qui ont un problème de ND
			OutilsPublication.insertCaracteristiqueEnvois(connection, l_idFichier, OutilsConstantes.CLE_NOMBRE_IC_PB_ND, Integer.toString(a_nbICPbND));

			// insertion des dates de publication des IC
			for (String l_datePubli : cDatePubli) {
				OutilsPublication.insertCaracteristiqueEnvois(connection, l_idFichier, OutilsConstantes.CLE_DATE_MVTS_TRAITE, l_datePubli);
			}
		}

		LOG.debug("insererDonnees - Fin");
	} // fin de insererDonnees

	/**
	 * updateNoSequence met à jour le champ NoSequence de la table
	 * InstanceSystèmeExterne.
	 * 
	 * @throws SQLException the SQL exception
	 */
	private void updateNoSequence() throws SQLException {
		LOG.debug("updateNoSequence - Debut");
		OutilsPublication.updateNoSequence(connection, instanceSE, noSequence + 1);
		LOG.debug("updateNoSequence - Fin");
	}

	/**
	 * terminerTraitementEnErreur termine le traitement en levant une exception.
	 */
	private void terminerTraitementEnErreur() {
		throw new Exception42C("Interruption Traitement Publication 42C");
	}

}
