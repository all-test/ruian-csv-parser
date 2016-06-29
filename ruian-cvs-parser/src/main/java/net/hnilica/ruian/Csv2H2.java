package net.hnilica.ruian;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.h2.tools.Backup;
import org.h2.tools.DeleteDbFiles;
import org.h2.tools.RunScript;

public class Csv2H2 {
	


	public static void main(String[] args) throws IOException, SQLException {
		String dbName="ruiandb";
		
		//File workDir=new File("/data/dracula/adresy");
		File workDir=new File("F:/adresy");
		//File workDir=new File("/media/dracula/KINGSTON");
		

		File okresFile=new File(workDir,"/UI_OKRES/UI_OKRES.csv");
		File obecOkresFile=new File(workDir,"/strukturovane-CSV/vazby-okresy-cr.csv");
		File mistaDir=new File(workDir,"/20151031_OB_ADR_csv/CSV");
		
		File beforeSqlFile=new File("src/main/resources/sql/before_import.sql");
		File afterSqlFile=new File("src/main/resources/sql/after_import.sql");

		
		final Reader readerOkres = new InputStreamReader(new FileInputStream(okresFile), "windows-1250");
		final CSVParser parserOkres = new CSVParser(readerOkres, CSVFormat.newFormat(';').withHeader().withIgnoreEmptyLines());
		
		HashMap<Long, String> okresHM1=new HashMap<>();
		try {
		    for (final CSVRecord record : parserOkres) {
		        final long okresKod = Long.parseLong(record.get("KOD"));
		    	final String okresNazev = record.get("NAZEV");
		    	//System.out.println("kod okresu: "+okresKod+", nazev: "+okresNazev);
		    	okresHM1.put(okresKod, okresNazev);
		    }
		} finally {
			parserOkres.close();
			readerOkres.close();
		}		
		
		
		final Reader readerObecOkres = new InputStreamReader(new FileInputStream(obecOkresFile), "windows-1250");
		final CSVParser parserObecOkres = new CSVParser(readerObecOkres, CSVFormat.newFormat(';').withHeader().withIgnoreEmptyLines());

		HashMap<Long, String> okresHM2obec=new HashMap<>();
		HashMap<Long, Long> okresHM3obec=new HashMap<>();
		//HashMap<Long, String> okresHM3cobce=new HashMap<>();
		
		
		try {
		    for (final CSVRecord record : parserObecOkres) {
		    	final long obecKod = Long.parseLong(record.get("OBEC_KOD"));
		    	final long cobceKod = Long.parseLong(record.get("COBCE_KOD"));
		    	final long okresKod = Long.parseLong(record.get("OKRES_KOD"));
		    	String okresNazev=okresHM1.get(okresKod);
		    	//System.out.println("kod okresu: "+okresKod+", kod obce: "+obecKod+", kod casti obce: "+cobceKod+", okres nazev: "+okresNazev);
		    	okresHM2obec.put(obecKod, okresNazev);
		    	okresHM3obec.put(obecKod, okresKod);
		    	//okresHM3cobce.put(cobceKod, okresNazev);
		    	
		    }
		} finally {
			parserObecOkres.close();
			readerObecOkres.close();
		}		
		
		
		DeleteDbFiles.execute(workDir.getAbsolutePath(), dbName, true);//smaze starsi databazi
		//Connection conn=DriverManager.getConnection("jdbc:h2:file:f:/test4;MODE=Oracle", "sa", "");
		Connection conn=DriverManager.getConnection("jdbc:h2:file:"+workDir.getAbsolutePath()+"/"+dbName+";MODE=Oracle", "sa", "");

		
		RunScript.execute(conn, new InputStreamReader(new FileInputStream(beforeSqlFile), "UTF-8"));


		String sql = "insert into vh_ruian (adm_kod,obec_kod,obec_nazev,obecmomc_nazev,obecmop_nazev,cobce_kod,cobce_nazev,ulice_nazev,typ_so,cislo_dom,cislo_or,cislo_or_pis,psc,okres_kod,okres_nazev) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		PreparedStatement ps = conn.prepareStatement(sql);
		 
		final int batchSize = 1000;
		int count = 0;
		 



		for(File mistoFile:mistaDir.listFiles()){
			System.out.println(mistoFile);
			final Reader readerMista = new InputStreamReader(new FileInputStream(mistoFile), "windows-1250");
			final CSVParser parserMista = new CSVParser(readerMista, CSVFormat.newFormat(';').withHeader().withIgnoreEmptyLines());

			try {
			    for (final CSVRecord record : parserMista) {
			    	final long admKod = Long.parseLong(record.get("Kód ADM"));
			    	final long obecKod = Long.parseLong(record.get("Kód obce"));
			    	final String obecNazev = record.get("Název obce");
			    	final String obecMOMCNazev = record.get("Název MOMC");//Název městského obvodu/městské části, který je vyplněn pouze v případě členěných statutárních měst
			    	final String obecMOPNazev = record.get("Název MOP");//Název městského obvodu Prahy, který je vyplněn pouze v případě Hlavního města Prahy.
			    	final long cobceKod = Long.parseLong(record.get("Kód části obce"));
			    	final String cobceNazev = record.get("Název části obce");
			    	final String uliceNazev = record.get("Název ulice");
			    	final String typSO = record.get("Typ SO");//Typ stavebního objektu, může nabývat hodnot: č.p. - číslo popisné stavebního objektu, č.ev. - číslo evidenční stavebního objektu
			    	final int cisloDom = Integer.parseInt(record.get("Číslo domovní"));
			    	final String cisloOrStr = record.get("Číslo orientační");
			    	final Integer cisloOr = cisloOrStr.isEmpty()?null:Integer.parseInt(record.get("Číslo orientační"));
			    	final String cisloOrpis = record.get("Znak čísla orientačního");
			        final String psc = record.get("PSČ");
			        
			        final Long okresKod=okresHM3obec.get(obecKod);
			        final String okresNazev=okresHM2obec.get(obecKod);


				    ps.setLong(1, admKod);
				    ps.setLong(2, obecKod);
				    ps.setString(3, obecNazev);
				    ps.setString(4, obecMOMCNazev);
				    ps.setString(5, obecMOPNazev);
				    ps.setLong(6, cobceKod);
				    ps.setString(7, cobceNazev);
				    ps.setString(8, uliceNazev);
				    ps.setString(9, typSO);
				    
				    ps.setInt(10, cisloDom);
				    ps.setObject(11, cisloOr);
				    ps.setString(12, cisloOrpis);
				    ps.setString(13, psc);
				    
				    ps.setLong(14, okresKod);
				    ps.setString(15, okresNazev);
				    ps.addBatch();
				     
				    if(++count % batchSize == 0) {
				        ps.executeBatch();
				    }
					
			    }
			} finally {
			    parserMista.close();
			    readerMista.close();
			}
			
			

		}

		ps.executeBatch(); // insert remaining records
		ps.close();
		
		RunScript.execute(conn, new InputStreamReader(new FileInputStream(afterSqlFile), "UTF-8"));
		
		conn.createStatement().execute("shutdown defrag");
	
		conn.close();		

		Backup.execute(workDir.getAbsolutePath()+"/"+dbName+".zip", workDir.getAbsolutePath(), dbName, false);
		
	}
}
