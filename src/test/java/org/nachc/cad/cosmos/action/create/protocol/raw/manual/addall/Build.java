package org.nachc.cad.cosmos.action.create.protocol.raw.manual.addall;

import org.nachc.cad.cosmos.action.create.protocol.raw.manual.BurnEverythingToTheGround;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.addall.thumb.AddAllDemoThumb;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.addall.thumb.AddAllDxThumb;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.addall.thumb.AddAllEncThumb;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.addall.thumb.AddAllFertThumb;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.addall.thumb.AddAllObsThumb;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.addall.thumb.AddAllOtherThumb;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.addall.thumb.AddAllProcThumb;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.addall.thumb.AddAllRxThumb;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.addall.thumb.CreateProject;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.update.grouptable.UpdateDemoGroupTable;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.update.grouptable.UpdateDiagGroupTable;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.update.grouptable.UpdateEncGroupTable;
import org.nachc.cad.cosmos.action.create.protocol.raw.manual.update.grouptable.UpdateRxGroupTable;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Build {

	public static void main(String[] args) {
		log("Burning everything to the ground.");
		BurnEverythingToTheGround.main(null);
		log.info("Adding Project");
		CreateProject.createProject();
		log("Demo");
		new AddAllDemoThumb().exec();
		log("Diagnosis");
		new AddAllDxThumb().exec();
		log("Encounter");
		new AddAllEncThumb().exec();
		log("Fertility");
		new AddAllFertThumb().exec();
		log("Observation");
		new AddAllObsThumb().exec();
		log("Other");
		new AddAllOtherThumb().exec();
		log("Procedure");
		new AddAllProcThumb().exec();
		log("Rx");
		new AddAllRxThumb().exec();
		log("Updates");
		new UpdateDemoGroupTable().doUpdate();
		new UpdateDiagGroupTable().doUpdate();
		new UpdateEncGroupTable().doUpdate();
		new UpdateRxGroupTable().doUpdate();
		log.info("Done.");
	}
	
	private static void log(String msg) {
		String rtn = "\n\n\n\n\n";
		rtn += "* ----------------------------------------------------------------- \n";
		rtn += "*\n";
		rtn += "* " + msg + "\n";
		rtn += "*\n";
		rtn += "* ----------------------------------------------------------------- \n";
		log.info(rtn);
	}
	
}
