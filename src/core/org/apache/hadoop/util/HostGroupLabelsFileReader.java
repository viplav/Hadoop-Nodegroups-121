/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.util;

import java.io.*;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

//Keeps track of which hosts are allowed for maps/reduces 

public class HostGroupLabelsFileReader {

  private Set<String> mapHostGroup;
  private Set<String> reduceHostGroup;
  private String hostGroupLabelsFile;
  private String mapHostGroupLabel;
  private String reduceHostGroupLabel;
  private Map<String, Set<String>> hostGroupsLabelsMap = 
	    new HashMap<String, Set<String>>();

   
  private static final Log LOG = LogFactory.getLog(HostGroupLabelsFileReader.class);

  public HostGroupLabelsFileReader() {	  
	mapHostGroup = null;
	reduceHostGroup = null;
	mapHostGroupLabel = "";
	reduceHostGroupLabel = "";	  
  }  

  private static void readXMLFileToMap(String labelsFile, 
		                               Map<String, Set<String>> hostGroupLabelsMap) 
                      throws IOException {
	   try {
		  URL url = Thread.currentThread().getContextClassLoader().getResource(labelsFile);
		  if (url == null) {
		      LOG.fatal("Host group labels file " + labelsFile + " not found");
		      throw new RuntimeException("Host group labels file " + labelsFile + " not found");
		  }
		  DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		  DocumentBuilder docBuilder = dbFactory.newDocumentBuilder();
		  Document doc = docBuilder.parse(url.toString());
		  if (doc == null) {
			  LOG.fatal ("Error parsing host group labels file " + labelsFile);
			  throw new RuntimeException ("Error parsing host group labels file " + labelsFile);
		  }

		  Element root = doc.getDocumentElement();
		  root.normalize();

		  if (!"CBD-NodeGroups".equalsIgnoreCase(root.getTagName())) {
			  LOG.fatal ("Bad node group labels file " + labelsFile);
			  throw new RuntimeException ("Bad node group labels file " + labelsFile);
		  }

		  NodeList nodeGroupLabelsList = root.getChildNodes();
		  for (int i=0; i < nodeGroupLabelsList.getLength(); i++) {
			  Node nodeGroupLabelNode = nodeGroupLabelsList.item(i);
			  if (!(nodeGroupLabelNode instanceof Element))
				  continue;
			  Element nodeGroupLabel = (Element) nodeGroupLabelNode;
			  if (!"NodeGroupLabel".equalsIgnoreCase(nodeGroupLabel.getTagName())) {
				  throw new RuntimeException("Bad CBD NodeGroups labels file - no NodeGroupLabel");	
			  }
			  String nodeGroupLabelAttr = nodeGroupLabel.getAttribute("label");

			  NodeList nodeList = nodeGroupLabel.getChildNodes();
			  Set<String> nodeSet = new HashSet<String>();
			  for (int j=0; j < nodeList.getLength(); j++) {
				  Node nodeNode = nodeList.item(j);
				  if (!(nodeNode instanceof Element))
					  continue;
				  Element nodeElem = (Element) nodeNode;
				  if ("Node".equalsIgnoreCase(nodeElem.getTagName())) {
					  nodeSet.add(nodeElem.getTextContent().trim());
				  }
			  }
			  hostGroupLabelsMap.put(nodeGroupLabelAttr.toLowerCase(), nodeSet);
		  }

       } catch (SAXException e) {
         LOG.fatal("error parsing conf file: " + e);
         throw new RuntimeException(e);
       } catch (ParserConfigurationException e) {
         LOG.fatal("error parsing conf file: " + e);
         throw new RuntimeException(e);
       }   
  }    
 
  public synchronized void refresh() throws IOException {
	 
	    readHostGroupLabels();
  }

  public synchronized void readHostGroupLabels() throws IOException {
	  if (!hostGroupLabelsFile.equals("")) {
		  LOG.info("Refreshing hostgroup labels for map/reduce tasks");
		  readXMLFileToMap(hostGroupLabelsFile, hostGroupsLabelsMap );
		  if (!mapHostGroupLabel.equals("")) {

			  mapHostGroup = hostGroupsLabelsMap.get(mapHostGroupLabel.toLowerCase());
		  }
		  if (!reduceHostGroupLabel.equals("")) {
			  reduceHostGroup = hostGroupsLabelsMap.get(reduceHostGroupLabel.toLowerCase());
		  }
	  }
  }

  public synchronized void setHostGroupLabelsFile(String hostGroupLabelsFile) {
	LOG.info("Setting the hostgroup labels file to : " + hostGroupLabelsFile);
	this.hostGroupLabelsFile = hostGroupLabelsFile;
  }

  public synchronized String getHostGroupLabelsFile() {
	return hostGroupLabelsFile;
  }

  public synchronized Set<String> getMapHostGroup() {
	return mapHostGroup;
  }

  public synchronized Set<String> getReduceHostGroup() {
    return reduceHostGroup;
  }

  public synchronized void setMapHostGroupLabel(String mapHostGroupLabel) {
    LOG.info("Using the map hostgroup label : " + mapHostGroupLabel);
    this.mapHostGroupLabel = mapHostGroupLabel;
  }
  
  public synchronized void setReduceHostGroupLabel(String reduceHostGroupLabel) {
    LOG.info("Using the reduce hostgroup label : " + reduceHostGroupLabel);
    this.reduceHostGroupLabel = reduceHostGroupLabel;
  }

  public synchronized void setMapReduceHostGroupLabels(String mapHostGroupLabel, 
                                           String reduceHostGroupLabel) 
                                           throws IOException {
    setMapHostGroupLabel(mapHostGroupLabel);
    setReduceHostGroupLabel(reduceHostGroupLabel);
  }

}
