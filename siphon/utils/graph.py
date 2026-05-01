"""General-purpose graph utilities."""

from __future__ import annotations

from collections import defaultdict


def topological_sort(
    nodes: list[str],
    edges: list[tuple[str, str]],
) -> list[str]:
    """Topologically sort nodes using Kahn's algorithm.

    Args:
        nodes: All node names to be sorted.
        edges: List of (parent, child) tuples — parent must come before child.
                Self-loops (parent == child) are silently ignored.

    Returns:
        A list of node names in topological order.

    Raises:
        ValueError: If the graph contains a cycle.
    """
    in_degree: dict[str, int] = {n: 0 for n in nodes}
    graph: dict[str, list[str]] = defaultdict(list)

    for parent, child in edges:
        if parent == child:
            continue
        graph[parent].append(child)
        in_degree[child] += 1

    queue = [n for n in nodes if in_degree[n] == 0]
    result: list[str] = []

    while queue:
        node = queue.pop(0)
        result.append(node)
        for neighbor in graph[node]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    if len(result) != len(nodes):
        raise ValueError("Cycle detected in graph")

    return result
