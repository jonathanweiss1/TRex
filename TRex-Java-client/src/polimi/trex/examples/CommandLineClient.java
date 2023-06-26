//
// This file is part of T-Rex, a Complex Event Processing Middleware.
// See http://home.dei.polimi.it/margara
//
// Authors: Gianpaolo Cugola, Daniele Rogora
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program.  If not, see http://www.gnu.org/licenses/.
//

package polimi.trex.examples;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.io.File;
import java.io.FileInputStream;
import java.util.Random;

import polimi.trex.common.Attribute;
import polimi.trex.common.Consts.EngineType;
import polimi.trex.communication.PacketListener;
import polimi.trex.communication.TransportManager;
import polimi.trex.packets.PubPkt;
import polimi.trex.packets.RulePkt;
import polimi.trex.packets.SubPkt;
import polimi.trex.packets.TRexPkt;
import polimi.trex.ruleparser.TRexRuleParser;

/**
 * WIE FUNKTIONIERT DAS HIER:
 * Eigentlich ganz einfach. EventTypes werden als int repräsentiert. Das macht man mit Assign in den TESLA Regeln.
 * Dann kann man events publishen und subscriben, indem man immer das jeweilige int angibt.
 * Der Einfachheit halber hab ich im Originalcode die ints am anfang einmal durch Variablen ersetzt, dann ist es besser lesbar.
 * 
 * Ich habe den kompletten Commandline Mist entfernt. Wenn ihr was am Code verändert, danach einfach nochmal compilieren mit "ant jars" und dann ausführen mit "java -jar TRex-client.jar"
 */


/**
 * @authors Gianpaolo Cugola, Daniele Rogora
 * 
 * A very basic, command line oriented, client for TRex.
 */
public class CommandLineClient implements PacketListener {
	// Jeder EventType wird durch eine id repräsentiert:
	static int SMOKE = 2000;
	static int TEMP = 2001;
	static int FIRE = 2100;

	// Das hier sind die beiden TESLA Regeln aus dem Paper. Der Code hat ursprünglich die Befehle aus einer Datei gelesen, deswegen müssen die escape-sequenzen rein:

	// static String R1_From = "From\t\tSmoke(area=$a) and each Temp(area=$a and value>45) within 5 min. from Smoke\r\n";
	// static String R1_Where = "Where\tarea=Smoke.area and measuredTemp=Temp.value";

	// static String R1 = assignment + definition + R1_From + R1_Where;

	private TransportManager tManager = new TransportManager(true);


    public static void main(String[] args) throws IOException {

		Random random = new Random();

		String serverHost = "localhost";
		int serverPort = 50254;
		CommandLineClient client;

		client = new CommandLineClient(serverHost, serverPort);
		client.tManager.addPacketListener(client);
		client.tManager.start();


		int TempCounter = 0;
		int FireCounter = 0;
		int SmokeCounter = 0;
		List<Integer> variableValues = new ArrayList<>();

		client.subscribe(Arrays.asList(new Integer[]{2001, 2100, 2000}));
		// create 1000 rules
		for (int i = 0; i < 1000; i++){
			// every 10th iteration, publish a new Fire rule
			if ((i + 1) % 10 == 0) {
				// Generate 10 random unifrom distributed variables
				int[] variables = new int[10];
				for (int j = 0; j < variables.length; j++) {
					variables[j] = random.nextInt(100) + 1;
					variableValues.add(variables[j]);
				}
				int[] TemperatureUniform = variables;
				for(int temp: TemperatureUniform){
					String assignment = String.format("Assign %d => Smoke, %d => Temp, %d => Fire\r\n", SMOKE, TEMP, FIRE);
					String definition = "Define\tFire(area: string, measuredTemp: int)\r\n";
					String R0_From = "From\t\tTemp(value>45)\r\n";
					R0_From = R0_From.replace("45", String.valueOf(temp));
					String R0_Where = "Where\tarea=Temp.area and measuredTemp=Temp.value";

					String R0 = assignment + definition + R0_From + R0_Where;
					client.sendRule(R0);
				}
			}
		}
		// create 1000 events
		int PublishEventCounter = 1000;
		double SmokeFactor = 0.9;
		for (int j = 0; j < PublishEventCounter; j++){
			if (j < PublishEventCounter*SmokeFactor) {
				// execute this 9000 times
				client.publish(TEMP, Arrays.asList(new String[]{"area", "value"}), Arrays.asList(new String[]{"1", "50"}));
			} else {
				// execute this 1000 times
				client.publish(TEMP, Arrays.asList(new String[]{"area", "value"}), Arrays.asList(new String[]{"1", "60"}));
			}
		}

		// ---------------------------------------------------------------------------------
		// Subscribe für Temp, Fire und Smoke events
		// client.subscribe(Arrays.asList(new Integer[]{2001, 2100, 2000}));
		// Regel für Middleware wird aktiviert
		// client.sendRule(R0);
		// Publish ein Temp event (2001) mit den Attributen area = 1 und value = 50
		// Die Middleware erkennt das Temp.value > 45 ist und sendet deswegen ein zusätzliches FIRE event aus
		// Leider wird nicht geprüft ob eine Regel schon existiert. Wenn das Programm also zweimal läuft ohne dazwischen den Server neu zu starten
		// werden dann 2 FIRE events erstellt.
		// client.publish(TEMP, Arrays.asList(new String[]{"area", "value"}), Arrays.asList(new String[]{"1", "50"}));
		// ----------------------------------------------------------------------------------
	}

    public CommandLineClient(String serverHost, int serverPort) throws IOException {
    	tManager.connect(serverHost, serverPort);
    }
 
    public void sendRule(String rule_) {
    	RulePkt rule = TRexRuleParser.parse(rule_, 2000);
    	try {
			tManager.sendRule(rule, EngineType.CPU);
		} catch (IOException e) { 
			e.printStackTrace();
		}
    }
    
    public void subscribe(List<Integer> subTypes) {
	for(int subType : subTypes) {
		SubPkt sub = new SubPkt(subType);
	    try {
		    tManager.send(sub);	
		 } catch (IOException e) { e.printStackTrace(); }
	    }
    }
    
    public void publish(int pubType, List<String> keys, List<String> values) {
	PubPkt pub;
	boolean boolVal;
	int intVal;
	float floatVal;

	pub = new PubPkt(pubType);
	for(int i=0; i<keys.size(); i++) {
	    if(values.get(i).equals("true")) {
		boolVal = true;
		pub.addAttribute(new Attribute(keys.get(i), boolVal)); // add a bool attr
	    } else if(values.get(i).equals("false")) {
		boolVal = false;
		pub.addAttribute(new Attribute(keys.get(i), boolVal)); // add a bool attr
	    } else {
		try {
		    intVal = Integer.parseInt(values.get(i));
		    pub.addAttribute(new Attribute(keys.get(i), intVal)); // add an int attr
		} catch(NumberFormatException e1) {
		    try {
			floatVal = Float.parseFloat(values.get(i));
			pub.addAttribute(new Attribute(keys.get(i), floatVal)); // add a float attr
		    } catch(NumberFormatException e2) {
			pub.addAttribute(new Attribute(keys.get(i), values.get(i))); // add a String attr
		    }
		}
	    }
	}
	try {
		System.out.println(pub);
	    tManager.send(pub);
	} catch (IOException e) { e.printStackTrace(); }	
    }

    @Override
    public void notifyPktReceived(TRexPkt pkt) {
	if(! (pkt instanceof PubPkt)) {
	    System.out.println("Ingnoring wrong packet: "+pkt);
	    return;
	}
	PubPkt pub = (PubPkt) pkt;
	System.out.print("PubPacket received: {");
	System.out.print(pub.getEventType());
	for(Attribute att : pub.getAttributes()) {
	    System.out.print(" <"+att.getName());
	    switch(att.getValType()) {
	    case BOOL: System.out.print(" : bool = "+att.getBoolVal()+">"); break;
	    case INT: System.out.print(" : int = "+att.getIntVal()+">"); break;
	    case FLOAT: System.out.print(" : float = "+att.getFloatVal()+">"); break;
	    case STRING: System.out.print(" : string = "+att.getStringVal()+">"); break;
	    }
	}
	System.out.print("}@");
	System.out.println(new Date(pub.getTimeStamp()).toLocaleString());
    }
    @Override
    public void notifyConnectionError() {
	System.out.println("Connection error. Exiting.");
	System.exit(-1);
    }
}
    
