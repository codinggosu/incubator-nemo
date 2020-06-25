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
package org.apache.nemo.runtime.executor.data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import java.util.ArrayList;
import java.lang.reflect.Field;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * A class to estimate the size of Objects.
 * necessary for implementing caching and determining memory status
 */
public final class SizeEstimator {
  private static final Logger LOG = LoggerFactory.getLogger(SizeEstimator.class.getName());
  private SizeEstimator() {
    // not called
  }

  // Sizes of primitive types
  private static final int BOOLEAN_SIZE = 1;
  private static final int CHAR_SIZE = 2;
  private static final int SHORT_SIZE = 2;
  private static final int INT_SIZE = 4;
  private static final int LONG_SIZE = 8;
  private static final int FLOAT_SIZE = 4;
  private static final int BYTE_SIZE = 1;
  private static final int DOUBLE_SIZE = 8;
  private static final int ALIGN_SIZE = 8;
  private static boolean is64Bit = true;
  private static int objectSize = 16; // 12 bytes with 8 byte offset


  // cache of classInfos
  private WeakHashMap<Class<?>, ClassInfo> classInfos = new WeakHashMap<Class<?>, ClassInfo>();


  // return a map keeping status of the count of fields of certain sizes
  private HashMap<Integer, Integer> getNewFieldSizesMap() {
    HashMap<Integer, Integer> fieldSizes = new HashMap<>();
    fieldSizes.put(1,0);
    fieldSizes.put(2,0);
    fieldSizes.put(4,0);
    fieldSizes.put(8,0);
    return fieldSizes;
//    return Stream.of(new Object[][] {
//      {  1, 0 },
//      {  2, 0 },
//      {  4, 0 },
//      {  8, 0 },
//    }).collect(Collectors.toMap(data -> (Integer) data[0], data -> (Integer) data[1]));
  }

  public static void initialize() {
    String architecture = System.getProperty("os.arch");
    is64Bit = (architecture.contains("64") || architecture.contains("s390x")) ? true : false;
//    LOG.info("the system architecture is {}", architecture);
    objectSize = is64Bit ? 16 : 8;
  }

  public static long estimate(final Object obj) {
    return estimate(obj, new IdentityHashMap());
  }

  public static long estimate(final Object obj, final IdentityHashMap map) {
    SearchState state = new SearchState(map);
    state.enque(obj);
    while (!state.isFinished()) {
      break;
    }
    return 0;
  }

  private void visitSingleObject(final Object obj, final SearchState state) {


  }

  private long alignSize(final long size, final int alignment) {
    return (size + alignment - 1) & ~(alignment - 1);
  }

  /**
   * A class.
   */
  private static class SearchState {
    private IdentityHashMap visited;
    private long size = 0L;
    private Stack<Object> stack;

    SearchState(final IdentityHashMap map) {
      this.visited = map;
      this.stack = new Stack<>();
    }


    void enque(final Object obj) {
      if (obj != null && !visited.containsKey(obj)) {
        visited.put(obj, null);
        stack.add(obj);
      }
    }

    boolean isFinished() {
      return stack.isEmpty();
    }

    Object deque() {
      return stack.pop();
    }
  }

  private long getPrimitiveSize(Class cls) {
    if (cls == Byte.class) {
      return BYTE_SIZE;
    } else if (cls == Boolean.class) {
      return BOOLEAN_SIZE;
    } else if (cls == Character.class) {
      return CHAR_SIZE;
    } else if (cls == Short.class) {
      return SHORT_SIZE;
    } else if (cls == Integer.class) {
      return INT_SIZE;
    } else if (cls == Long.class) {
      return LONG_SIZE;
    } else if (cls == Float.class) {
      return FLOAT_SIZE;
    } else if (cls == Double.class) {
      return DOUBLE_SIZE;
    } else {
      throw new IllegalArgumentException(
        "Non-primitive class " + cls + " passed to primitiveSize()");
    }
  }

  /**
   * A class for ClassInfo, which contains the class overhead size and references the class has.
   */
  private class ClassInfo {
    private long shellSize;
    private List<Field> pointerFields;
  }

  /**
   * Get or compute the ClassInfo for a given class.
   */
  private ClassInfo getClassInfo(Class<?> cls) {
    // Check whether we've already cached a ClassInfo for this class
    ClassInfo info = classInfos.get(cls);
    if (info != null) {
      return info;
    }

    ClassInfo parent = getClassInfo(cls.getSuperclass());
    long shellSize = parent.shellSize;
    List pointerFields = parent.pointerFields;
    HashMap sizeCount = getNewFieldSizesMap();

    // iterate through the fields of this class and gather information.
    for (Field field : cls.getDeclaredFields()) {
      if (java.lang.reflect.Modifier.isStatic(field.getModifiers())) {
        Class fieldClass = field.getType();
        if (fieldClass.isPrimitive()) {
          sizeCount.put(getPrimitiveSize(fieldClass),
            sizeCount.get(getPrimitiveSize((fieldClass))));
        } else {
          try {
            field.setAccessible(true); // Enable future get()'s on this field
            pointerFields.addAll(field.pointerFields);
          } catch (Exception RuntimeException){
            LOG.error("Error when trying to determine size of a filed in class {}", cls);
          }
          sizeCount(pointerSize) += 1;
        }
      }
    }

    // Based on the simulated field layout code in Aleksey Shipilev's report:
    // http://cr.openjdk.java.net/~shade/papers/2013-shipilev-fieldlayout-latest.pdf
    // The code is in Figure 9.
    // The simplified idea of field layout consists of 4 parts (see more details in the report):
    //
    // 1. field alignment: HotSpot lays out the fields aligned by their size.
    // 2. object alignment: HotSpot rounds instance size up to 8 bytes
    // 3. consistent fields layouts throughout the hierarchy: This means we should layout
    // superclass first. And we can use superclass's shellSize as a starting point to layout the
    // other fields in this class.
    // 4. class alignment: HotSpot rounds field blocks up to HeapOopSize not 4 bytes, confirmed
    // with Aleksey. see https://bugs.openjdk.java.net/browse/CODETOOLS-7901322
    //
    // The real world field layout is much more complicated. There are three kinds of fields
    // order in Java 8. And we don't consider the @contended annotation introduced by Java 8.
    // see the HotSpot classloader code, layout_fields method for more details.
    // hg.openjdk.java.net/jdk8/jdk8/hotspot/file/tip/src/share/vm/classfile/classFileParser.cpp
    var alignedSize = shellSize
    for (size <- fieldSizes if sizeCount(size) > 0) {
      val count = sizeCount(size).toLong
      // If there are internal gaps, smaller field can fit in.
      alignedSize = math.max(alignedSize, alignSizeUp(shellSize, size) + size * count)
      shellSize += size * count
    }

    // Should choose a larger size to be new shellSize and clearly alignedSize >= shellSize, and
    // round up the instance filed blocks
    shellSize = alignSizeUp(alignedSize, pointerSize)

    // Create and cache a new ClassInfo
    val newInfo = new ClassInfo(shellSize, pointerFields)
    classInfos.put(cls, newInfo)
    newInfo
  }

}






















