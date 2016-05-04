package org.gradoop.model.impl.operators.simulation.common.query;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.s1ck.gdl.GDLHandler;
import org.s1ck.gdl.model.Edge;
import org.s1ck.gdl.model.GraphElement;
import org.s1ck.gdl.model.Vertex;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Wraps a {@link GDLHandler} and adds functionality needed for query
 * processing.
 */
public class QueryHandler {

  private final GDLHandler gdlHandler;

  private Map<Long, Vertex> idToVertexCache;

  private Map<Long, Edge> idToEdgeCache;

  private Map<String, Set<Vertex>> labelToVertexCache;

  private Map<String, Set<Edge>> labelToEdgeCache;

  private Map<Long, Set<Edge>> sourceIdToEdgeCache;

  private Map<Long, Set<Edge>> targetIdToEdgeCache;

  private QueryHandler(String gdlString) {
    gdlHandler = new GDLHandler.Builder().buildFromString(gdlString);
  }

  public static QueryHandler fromString(String gdlString) {
    return new QueryHandler(gdlString);
  }

  public Collection<Vertex> getVertices() {
    return gdlHandler.getVertices();
  }

  public Collection<Edge> getEdges() {
    return gdlHandler.getEdges();
  }

  public Vertex getVertexById(Long id) {
    if (idToVertexCache == null) {
      idToVertexCache = initIdToElementCache(gdlHandler.getVertices());
    }
    return idToVertexCache.get(id);
  }

  public Edge getEdgeById(Long id) {
    if (idToEdgeCache == null) {
      idToEdgeCache = initIdToElementCache(gdlHandler.getEdges());
    }
    return idToEdgeCache.get(id);
  }

  public Collection<Vertex> getVerticesByLabel(String label) {
    if (labelToVertexCache == null) {
      initLabelToVertexCache();
    }
    return labelToVertexCache.get(label);
  }

  public Collection<Edge> getEdgesByLabel(String label) {
    if (labelToEdgeCache == null) {
      initLabelToEdgeCache();
    }
    return labelToEdgeCache.get(label);
  }

  public Collection<Edge> getEdgesBySourceVertexId(Long id) {
    if (sourceIdToEdgeCache == null) {
      initSourceIdToEdgeCache();
    }
    return sourceIdToEdgeCache.get(id);
  }

  public Collection<Edge> getEdgesByTargetVertexId(Long id) {
    if (targetIdToEdgeCache == null) {
      initTargetIdToEdgeCache();
    }
    return targetIdToEdgeCache.get(id);
  }

  public Collection<Edge> getPredecessors(Long edgeId) {
    Collection<Edge> predecessors =
      getEdgesByTargetVertexId(getEdgeById(edgeId).getSourceVertexId());
    return predecessors != null ?
      Lists.newArrayList(predecessors) : Lists.<Edge>newArrayList();
  }

  public Collection<Edge> getSuccessors(Long edgeId) {
    Collection<Edge> successors =
      getEdgesBySourceVertexId(getEdgeById(edgeId).getTargetVertexId());
    return successors != null ?
      Lists.newArrayList(successors) : Lists.<Edge>newArrayList();
  }

  public Collection<Long> getPredecessorIds(Long edgeId) {
    return getIds(getPredecessors(edgeId));
  }

  public Collection<Long> getSuccessorIds(Long edgeId) {
    return getIds(getSuccessors(edgeId));
  }

  private <EL extends GraphElement> Collection<Long>
  getIds(Collection<EL> elements) {
    List<Long> ids = Lists.newArrayListWithCapacity(elements.size());
    for (EL el : elements) {
      ids.add(el.getId());
    }
    return ids;
  }

  private <EL extends GraphElement> Map<Long, EL>
  initIdToElementCache(Collection<EL> elements) {
    Map<Long, EL> cache = Maps.newHashMap();
    for (EL el : elements) {
      cache.put(el.getId(), el);
    }
    return cache;
  }

  private void initLabelToVertexCache() {
    labelToVertexCache = initLabelToGraphElementCache(getVertices());
  }

  private void initLabelToEdgeCache() {
    labelToEdgeCache = initLabelToGraphElementCache(getEdges());
  }

  private void initSourceIdToEdgeCache() {
    sourceIdToEdgeCache = initVertexToEdgeCache(gdlHandler.getEdges(), true);
  }

  private void initTargetIdToEdgeCache() {
    targetIdToEdgeCache = initVertexToEdgeCache(gdlHandler.getEdges(), false);
  }

  private Map<Long, Set<Edge>>
  initVertexToEdgeCache(Collection<Edge> edges, boolean useSource) {
    Map<Long, Set<Edge>> cache = Maps.newHashMap();
    for (Edge e : edges) {
      Long vId = useSource ? e.getSourceVertexId() : e.getTargetVertexId();

      if (cache.containsKey(vId)) {
        cache.get(vId).add(e);
      } else {
        cache.put(vId, Sets.newHashSet(e));
      }
    }
    return cache;
  }

  private <EL extends GraphElement> Map<String, Set<EL>>
  initLabelToGraphElementCache(Collection<EL> elements) {
    Map<String, Set<EL>> cache = Maps.newHashMap();
    for (EL el : elements) {
      if (cache.containsKey(el.getLabel())) {
        cache.get(el.getLabel()).add(el);
      } else {
        Set<EL> set = new HashSet<>();
        set.add(el);
        cache.put(el.getLabel(), set);
      }
    }
    return cache;
  }
}
