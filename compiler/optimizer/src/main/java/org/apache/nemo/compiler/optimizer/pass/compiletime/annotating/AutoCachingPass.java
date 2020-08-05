/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.compiler.optimizer.pass.compiletime.annotating;

import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.edge.IREdge;
// import org.apache.nemo.common.ir.edge.executionproperty.CachingProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DataPersistenceProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DataStoreProperty;
import org.apache.nemo.compiler.optimizer.pass.compiletime.Requires;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.Pair;

/**
 * Pass for initiating IREdge data persistence ExecutionProperty with default
 * values.
 */
@Annotates(DataPersistenceProperty.class)
@Requires(DataStoreProperty.class)
public final class AutoCachingPass extends AnnotatingPass {
    private static final Logger LOG = LoggerFactory.getLogger(AutoCachingPass.class.getName());

    /**
     * Default constructor.
     */
    public AutoCachingPass() {
        super(AutoCachingPass.class);
    }



    @Override
    public IRDAG apply(final IRDAG dag) {
      // a map to store which edge is the result of which vertex + the input to that vertex
      // used to track whether an edge is reused, and whether and edge is the same as another edge
      HashMap<IREdge, Pair<IRVertex, List<IREdge>>> edgeSources = new HashMap<>();

      // populate { edgeSources } first by iterating through vertices and putting the output to edgeSources map
      
      dag.getVertices().forEach(vertex ->
        dag.getIncomingEdgesOf(vertex).stream()
          .filter(edge -> !edge.getPropertyValue(DataStoreProperty.class).isPresent())
          .forEach(edge -> edge.setProperty(
            DataStoreProperty.of(DataStoreProperty.Value.LOCAL_FILE_STORE))));

      // previous implementation of autocaching (wrong implementation, failed)
        // HashMap<IRVertex, Integer> locations = new HashMap<IRVertex, Integer>();
        // make hashmap of transformations and their vertex ids
//        final List<IRVertex> allVertices = dag.getVertices();
//        HashMap<String, ArrayList<IRVertex>> transformOrder = new HashMap<String, ArrayList<IRVertex>>();
//        for (final IRVertex vertex : allVertices) {
//            String passname = "";
//            try {
//                OperatorVertex opvertex = (OperatorVertex) vertex;
//                passname = getPassName(opvertex);
//                if (transformOrder.containsKey(passname)) {
//                    transformOrder.get(passname).add(opvertex);
//                } else {
//                    ArrayList<IRVertex> value = new ArrayList<IRVertex>();
//                    value.add(opvertex);
//                    transformOrder.put(passname, value);
//                }
//            } catch (Exception e) {
//                continue;
//            }
//        }
//        //iterate over each transformation and make subdag to compare
//        for (String passName : transformOrder.keySet()) {
//            ArrayList<IRVertex> vertices = transformOrder.get(passName);
//            // ArrayList<IRVertex> beginEnd = new ArrayList<IRVertex>(2);
//            IRVertex[] beginEnd = new IRVertex[2];
//            // ArrayList<ArrayList<IRVertex>> subDags = new ArrayList<ArrayList<IRVertex>>();
//            for (int i = 0; i < vertices.size(); i++) {
//                for (int j = i + 1; j < vertices.size(); j++) {
//                    beginEnd[0] = vertices.get(i);
//                    beginEnd[1] = vertices.get(j);
//                    // beginEnd.set(0, vertices.get(i));
//                    // beginEnd.set(1, vertices.get(j));
//                    // subDags.add(beginEnd);
//                    LOG.info("begin {} end {} ", beginEnd[0].getNumericId(), beginEnd[1].getNumericId());
//                    List<ArrayList<IRVertex>> test = getAllPathsBetween(dag, beginEnd[0], beginEnd[1]);
//                    LOG.info("PASS NAME {} ", passName);
//                    LOG.info("begin  {} end {} ", beginEnd[0], beginEnd[1]);
//                    LOG.info("all paths length {} ", test.size());
//                    for (ArrayList<IRVertex> member : test) {
//                        LOG.info("first element {}, last element {}",  member.get(0), member.get(member.size() - 1));
//                        // for (IRVertex elem : member) {
//                        //     LOG.info("PATH FROM {} to {}", beginEnd.get(0).toString(), beginEnd.get(1).toString());
//                        //     LOG.info("element {}, num id {}", elem.toString(), elem.getNumericId());
//                        // }
//                    }
//                    // LOG.info("BeginEnd for transformation {}, {}", passName, beginEnd);
//                }
//            }
//            // for (int i = 0; i < subDags.size(); i++) {
//            //     for (int j = i + 1; j < subDags.size(); j++) {
//            //         IREdge marked = markEqualOperationSubdag(dag, subDags.get(i), subDags.get(j));
//            //         if (marked != null) {
//            //             // marked.getPropertyValue(CachingProperty.class);
//            //             // marked.set
//            //             LOG.info("the ID OF MARKED IS  !!!!! {}", marked.getNumericId());
//            //         }
//            //     }
//            // }
//        }
        return dag;
    }
    /**
     * Return all path between two verticies if it exists.
     * @param dag dag
     * @param v1 source
     * @param v2 destination
     * @param paths all paths empty
     * @return paths all paths filled
     */
    public List<ArrayList<IRVertex>> getAllPathsBetween(final IRDAG dag, final IRVertex v1, final IRVertex v2) {
        ArrayList<ArrayList<IRVertex>> allPaths = new ArrayList<ArrayList<IRVertex>>();
        if (!dag.pathExistsBetween(v1, v2)) {
            return allPaths;
        }
        HashSet<IRVertex> visited = new HashSet<IRVertex>();
        ArrayList<IRVertex> currentPath = new ArrayList<IRVertex>();
        // currentPath.add(v1);
        currentPath = addPathtoAllPaths(dag, v1, v2, visited, currentPath);
        if (currentPath != null && currentPath.size() > 0) {
            allPaths.add(currentPath);
        }
        return allPaths;
    }
    /**
     * Util function to add path.
     * @param dag         dag
     * @param source      source
     * @param destination destination
     * @param allPaths    all the paths
     * @param currentPath current path to add to
     */
    public ArrayList<IRVertex> addPathtoAllPaths(final IRDAG dag, final IRVertex source, final IRVertex destination,
            final HashSet<IRVertex> visited, final ArrayList<IRVertex> currentPath) {
        // ArrayList<IRVertex> currentPath = new ArrayList<IRVertex>();
        visited.add(source);
        if (source.equals(destination)) {
            return currentPath;
        }
        for (IRVertex newSource : dag.getDescendants(source.getId())) {
            if (!visited.contains(newSource)) {
                currentPath.add(newSource);
                addPathtoAllPaths(dag, newSource, destination, visited, currentPath);
                // currentPath.remove(newSource);
            }
        }
        if (currentPath.get(currentPath.size() - 1).equals(destination)) {
            return currentPath;
        }
        return null;
        //visited.remove(v1);
    }
    /**
     * Check if two subdags are the same in terms of operations performed.
     * @param IRDAG dag
     * @param subDagOne one
     * @param subDagTwo two
     * @return IREdge to mark as cache
     */
    public IREdge markEqualOperationSubdag(final IRDAG dag, final IRVertex[] subDagOne, final IRVertex[] subDagTwo) {
        // if there are no paths between the two
        if (!dag.pathExistsBetween(subDagOne[0], subDagOne[1])  || dag.pathExistsBetween(subDagTwo[0], subDagTwo[1])) {
            return null;
        }
        // if the ending of one path is not the start of another path
        LinkedList<IRVertex> dagOne = new LinkedList<IRVertex>();
        LinkedList<IRVertex> dagTwo = new LinkedList<IRVertex>();
        for (IRVertex vertex : dag.getVertices()) {
            if (vertex.equals(subDagOne[0]) || vertex.equals(subDagOne[1])) {
                if (vertex.equals(subDagOne[0])) {
                    continue;
                } else if (vertex.equals(subDagOne[1])) {
                    continue;
                }
            }
            continue;
        }
        return null;
    }

    /**
     * Get pass name.
     * @param vertex a vertex
     * @return passname
     */
    public String getPassName(final IRVertex vertex) {
        String passName;
        final Pattern passNameRegex = Pattern.compile("(.+Transform)");
        try {
            OperatorVertex opvertex = (OperatorVertex) vertex;
            // opvertex.exe
            Matcher m = passNameRegex.matcher(opvertex.getTransform().toString());
            m.find();
            passName = m.group(1);
        } catch (Exception e) {
            // pass empty vertices
            return "";
        }
        return passName;
    }
}
