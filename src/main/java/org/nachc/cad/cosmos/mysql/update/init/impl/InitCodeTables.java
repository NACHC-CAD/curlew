package org.nachc.cad.cosmos.mysql.update.init.impl;

import java.nio.file.spi.FileTypeDetector;
import java.sql.Connection;
import java.util.Date;

import org.nachc.cad.cosmos.dvo.mysql.cosmos.DocumentRoleDvo;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.FileTypeDvo;
import org.nachc.cad.cosmos.dvo.mysql.cosmos.StatusDvo;
import org.nachc.cad.cosmos.proxy.mysql.cosmos.PersonProxy;
import org.yaorma.dao.Dao;
import org.yaorma.util.time.TimeUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InitCodeTables {

	public static void init(Connection conn) {
		initStatus(conn);
		initFileType(conn);
		initDocumentRole(conn);
	}
	
	//
	// status
	//
	
	private static void initStatus(Connection conn) {
		StatusDvo dvo = new StatusDvo();
		Date now = TimeUtil.getNow();
		String createdBy = PersonProxy.getGuidForUid("greshje", conn);
		dvo.setCreatedBy(createdBy);
		dvo.setUpdatedBy(createdBy);
		dvo.setCreatedDate(now);
		dvo.setUpdatedDate(now);
		initStatus("DRAFT", "Draft", "Draft version of this resource.  The resource is exclued from production data sets.", dvo, conn);
		initStatus("PENDING", "Pending", "Resource is waiting approval.  The resource is excluded from production data sets.", dvo, conn);
		initStatus("PROD", "Production", "The resource has been validated and approved.  The resource is included in production data sets.", dvo, conn);
		initStatus("DELETED", "Deleted", "The resource has been deleted (not really, it can be recovered by the admin.", dvo, conn);
	}
	
	private static void initStatus(String code, String name, String desc, StatusDvo dvo, Connection conn) {
		dvo.setCode(code);
		dvo.setDescription(desc);
		dvo.setName(name);
		log.info("Inserting status: " + code);
		Dao.insert(dvo, conn);
	}
	
	//
	// file type
	// 
	
	private static void initFileType(Connection conn) {
		FileTypeDvo dvo = new FileTypeDvo();
		Date now = TimeUtil.getNow();
		String createdBy = PersonProxy.getGuidForUid("greshje", conn);
		dvo.setCreatedBy(createdBy);
		dvo.setUpdatedBy(createdBy);
		dvo.setCreatedDate(now);
		dvo.setUpdatedDate(now);
		initFileType("XLS", "Excel Spreadsheet", "Excel Spreadsheet", dvo, conn);
		initFileType("CSV", "Character Separated Values", "Can be delimited by any character (e.g. pipe delimited etc.)", dvo, conn);
		initFileType("TXT", "Text file", "A text file that is not character delimited.", dvo, conn);
		initFileType("PDF", "Portable Data Format", "PDF file", dvo, conn);
		initFileType("WORD", "MS Word", "Microsoft word document", dvo, conn);
		initFileType("PPT", "MS PowerPoint", "PowerPoint presentation", dvo, conn);
	}
	
	private static void initFileType(String code, String name, String desc, FileTypeDvo dvo, Connection conn) {
		dvo.setCode(code);
		dvo.setName(name);
		dvo.setDescription(desc);
		log.info("Inserting File Type: " + code);
		Dao.insert(dvo, conn);
	}
	
	//
	// document role
	//
	
	private static void initDocumentRole(Connection conn) {
		DocumentRoleDvo dvo = new DocumentRoleDvo();
		Date now = TimeUtil.getNow();
		String createdBy = PersonProxy.getGuidForUid("greshje", conn);
		dvo.setCreatedBy(createdBy);
		dvo.setUpdatedBy(createdBy);
		dvo.setCreatedDate(now);
		dvo.setUpdatedDate(now);
		initDocumentRole("DATA", "Data File", "This file is used directly to populate a Databricks data source.", dvo, conn);
		initDocumentRole("SOURCE", "Source File", "This file was provided by the indicated source. It is not used directly as a data source (it is generally parsed and the parsed file is used as the data source.", dvo, conn);
		initDocumentRole("INFO", "Information File", "This file provides additional documentation about the data. It is not part of the data workflow directly.", dvo, conn);
		initDocumentRole("PROTOCOL", "Protocol", "This is the protocol document for this data block.", dvo, conn);
	}
	
	private static void initDocumentRole(String code, String name, String desc, DocumentRoleDvo dvo, Connection conn) {
		dvo.setCode(code);
		dvo.setName(name);
		dvo.setDescription(desc);
		log.info("Inserting Document Role: " + code);
		Dao.insert(dvo, conn);
	}
	
}
