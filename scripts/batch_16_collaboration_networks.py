#!/usr/bin/env python3
"""
================================================================================
CINESCOPE BATCH 16: COLLABORATION NETWORKS ANALYSIS
================================================================================

Focus: Who works together repeatedly, degrees of separation, co-star networks

Data Sources:
- Cast data across all films
- Director-actor relationships
- Collaboration frequency

Visualizations (12):
1. actor_network_graph.png - Main co-appearance network
2. degrees_of_separation_distribution.png - Kevin Bacon style analysis
3. top_connected_actors.png - Centrality ranking
4. collaboration_frequency_heatmap.png - Who works together most
5. director_actor_network.png - Director-actor collaborations
6. network_communities.png - Actor cliques/communities
7. clustering_coefficient_distribution.png - Network clustering
8. betweenness_centrality.png - Bridge actors
9. collaboration_strength_vs_rating.png - Does frequent collaboration = quality?
10. network_density_over_time.png - How networks evolve
11. top_collaborating_pairs.png - Most frequent partnerships
12. ego_network_top_actor.png - Network around most connected actor

================================================================================
"""

import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from datetime import datetime
from collections import defaultdict, Counter
import networkx as nx
from itertools import combinations
import community as community_louvain
import ast
import plotly.graph_objects as go
import plotly.express as px

class CollaborationNetworkAnalyzer:
    """Analyzes collaboration networks in cinema."""

    def __init__(self):
        """Initialize the analyzer."""
        self.base_dir = Path(__file__).parent.parent
        self.data_dir = self.base_dir / 'data'
        self.output_dir = self.base_dir / 'analysis_outputs'
        self.viz_dir = self.output_dir / 'visualizations' / 'batch_16'
        self.reports_dir = self.output_dir / 'reports'

        # Create output directories
        self.viz_dir.mkdir(parents=True, exist_ok=True)
        self.reports_dir.mkdir(parents=True, exist_ok=True)

        # Set visualization style
        sns.set_style("whitegrid")
        plt.rcParams['figure.figsize'] = (12, 8)
        plt.rcParams['font.size'] = 10

        # Data containers
        self.people_cache = {}
        self.movies_df = None
        self.collaboration_graph = nx.Graph()
        self.director_actor_graph = nx.Graph()
        self.stats = {}

        print("="*80)
        print("CINESCOPE BATCH 16: COLLABORATION NETWORKS ANALYSIS")
        print("="*80)
        print()

    def load_data(self):
        """Load all required data files."""
        print("Loading data files...")

        # Load people cache
        people_cache_path = self.data_dir / 'processed' / 'people_cache.json'
        with open(people_cache_path, 'r', encoding='utf-8') as f:
            self.people_cache = json.load(f)
        print(f"✓ Loaded {len(self.people_cache):,} people from cache")

        # Load movies
        movies_path = self.data_dir / 'processed' / 'watched_movies_master.csv'
        self.movies_df = pd.read_csv(movies_path)
        print(f"✓ Loaded {len(self.movies_df):,} movies")

        print()

    def build_collaboration_networks(self):
        """Build collaboration networks from cast data."""
        print("Building collaboration networks...")

        # Build cache_id to imdb_id mapping
        cache_id_to_imdb = {}
        imdb_to_name = {}

        for cache_id, person_data in self.people_cache.items():
            imdb_id = person_data.get('imdb_id')
            name = person_data.get('imdb_name', 'Unknown')
            if imdb_id:
                cache_id_to_imdb[cache_id] = str(imdb_id).strip()
                imdb_to_name[str(imdb_id).strip()] = name

        # Track collaborations
        actor_collaborations = defaultdict(lambda: defaultdict(list))
        director_actor_collaborations = defaultdict(lambda: defaultdict(list))

        movies_processed = 0

        for _, movie in self.movies_df.iterrows():
            movie_title = movie.get('Title', 'Unknown')
            movie_year = movie.get('Year', '')
            movie_rating = movie.get('IMDb Rating', 0)

            # Get cast IDs
            cast_ids = []
            imdb_cast_str = movie.get('imdb_cast_ids', '')
            if imdb_cast_str and not pd.isna(imdb_cast_str):
                try:
                    cast_ids_list = ast.literal_eval(str(imdb_cast_str))
                    if isinstance(cast_ids_list, list):
                        cast_ids.extend([str(id).strip() for id in cast_ids_list if id])
                except:
                    cast_ids.extend([id.strip() for id in str(imdb_cast_str).split('|') if id.strip()])

            # Get director IDs
            director_ids = []
            imdb_director_str = movie.get('imdb_director_ids', '')
            if imdb_director_str and not pd.isna(imdb_director_str):
                try:
                    director_ids_list = ast.literal_eval(str(imdb_director_str))
                    if isinstance(director_ids_list, list):
                        director_ids.extend([str(id).strip() for id in director_ids_list if id])
                except:
                    director_ids.extend([id.strip() for id in str(imdb_director_str).split('|') if id.strip()])

            # Normalize IDs to strings and strip whitespace
            cast_ids = [str(id).strip() for id in cast_ids]
            director_ids = [str(id).strip() for id in director_ids]

            # Record actor-actor collaborations
            if len(cast_ids) >= 2:
                for actor1, actor2 in combinations(cast_ids, 2):
                    if actor1 in imdb_to_name and actor2 in imdb_to_name:
                        actor_collaborations[actor1][actor2].append({
                            'movie': movie_title,
                            'year': movie_year,
                            'rating': movie_rating
                        })
                        actor_collaborations[actor2][actor1].append({
                            'movie': movie_title,
                            'year': movie_year,
                            'rating': movie_rating
                        })

            # Record director-actor collaborations
            for director_id in director_ids:
                for actor_id in cast_ids:
                    if director_id in imdb_to_name and actor_id in imdb_to_name:
                        director_actor_collaborations[director_id][actor_id].append({
                            'movie': movie_title,
                            'year': movie_year,
                            'rating': movie_rating
                        })

            movies_processed += 1

        # Build NetworkX graphs
        print("  Building actor collaboration graph...")
        for actor1, collaborators in actor_collaborations.items():
            for actor2, movies in collaborators.items():
                if actor1 < actor2:  # Avoid duplicate edges
                    weight = len(movies)
                    avg_rating = np.mean([m['rating'] for m in movies if m['rating'] > 0])
                    self.collaboration_graph.add_edge(
                        actor1,
                        actor2,
                        weight=weight,
                        avg_rating=avg_rating,
                        movies=movies
                    )

        print("  Building director-actor graph...")
        for director_id, actors in director_actor_collaborations.items():
            for actor_id, movies in actors.items():
                weight = len(movies)
                avg_rating = np.mean([m['rating'] for m in movies if m['rating'] > 0])
                self.director_actor_graph.add_edge(
                    director_id,
                    actor_id,
                    weight=weight,
                    avg_rating=avg_rating,
                    movies=movies
                )

        # Store name mapping
        self.imdb_to_name = imdb_to_name

        # Calculate stats
        self.stats['movies_processed'] = movies_processed
        self.stats['total_actors'] = self.collaboration_graph.number_of_nodes()
        self.stats['total_collaborations'] = self.collaboration_graph.number_of_edges()
        self.stats['network_density'] = nx.density(self.collaboration_graph)

        # Get largest connected component
        if self.collaboration_graph.number_of_nodes() > 0:
            largest_cc = max(nx.connected_components(self.collaboration_graph), key=len)
            self.largest_component = self.collaboration_graph.subgraph(largest_cc).copy()
            self.stats['largest_component_size'] = len(largest_cc)
        else:
            self.largest_component = self.collaboration_graph
            self.stats['largest_component_size'] = 0

        print(f"✓ Built actor network: {self.stats['total_actors']:,} actors, {self.stats['total_collaborations']:,} collaborations")
        print(f"✓ Network density: {self.stats['network_density']:.4f}")
        print(f"✓ Largest connected component: {self.stats['largest_component_size']:,} actors")
        print(f"✓ Director-actor network: {self.director_actor_graph.number_of_nodes():,} people, {self.director_actor_graph.number_of_edges():,} collaborations")
        print()

    def calculate_network_metrics(self):
        """Calculate various network centrality metrics."""
        print("Calculating network metrics...")

        if self.largest_component.number_of_nodes() == 0:
            print("! No connected component to analyze")
            return

        # Degree centrality
        self.degree_centrality = nx.degree_centrality(self.largest_component)

        # Betweenness centrality (on sample for speed)
        if self.largest_component.number_of_nodes() > 500:
            sample_nodes = np.random.choice(
                list(self.largest_component.nodes()),
                size=500,
                replace=False
            )
            self.betweenness_centrality = nx.betweenness_centrality(
                self.largest_component,
                k=500
            )
        else:
            self.betweenness_centrality = nx.betweenness_centrality(self.largest_component)

        # Clustering coefficient
        self.clustering = nx.clustering(self.largest_component)

        # Community detection
        print("  Detecting communities...")
        self.communities = community_louvain.best_partition(self.largest_component)
        num_communities = len(set(self.communities.values()))

        self.stats['num_communities'] = num_communities
        self.stats['avg_clustering'] = np.mean(list(self.clustering.values()))

        print(f"✓ Calculated centrality metrics")
        print(f"✓ Detected {num_communities} communities")
        print(f"✓ Average clustering coefficient: {self.stats['avg_clustering']:.4f}")
        print()

    def visualize_actor_network(self):
        """Visualize main actor collaboration network (top actors only)."""
        print("Creating actor network visualization...")

        if self.largest_component.number_of_nodes() == 0:
            print("! No network to visualize")
            return

        # Get top 100 actors by degree centrality
        top_actors = sorted(
            self.degree_centrality.items(),
            key=lambda x: x[1],
            reverse=True
        )[:100]

        top_actor_ids = [actor_id for actor_id, _ in top_actors]
        subgraph = self.largest_component.subgraph(top_actor_ids)

        fig, ax = plt.subplots(figsize=(16, 16))

        # Node sizes based on degree centrality
        node_sizes = [self.degree_centrality[node] * 5000 for node in subgraph.nodes()]

        # Node colors based on community
        node_colors = [self.communities.get(node, 0) for node in subgraph.nodes()]

        # Edge widths based on collaboration frequency
        edge_widths = [subgraph[u][v]['weight'] * 0.5 for u, v in subgraph.edges()]

        # Use spring layout
        pos = nx.spring_layout(subgraph, k=2, iterations=50, seed=42)

        # Draw network
        nx.draw_networkx_edges(
            subgraph, pos,
            width=edge_widths,
            alpha=0.2,
            edge_color='gray',
            ax=ax
        )

        nx.draw_networkx_nodes(
            subgraph, pos,
            node_size=node_sizes,
            node_color=node_colors,
            cmap='tab20',
            alpha=0.7,
            ax=ax
        )

        # Label top 20 actors
        top_20_ids = [actor_id for actor_id, _ in top_actors[:20]]
        labels = {node: self.imdb_to_name.get(node, 'Unknown')
                  for node in top_20_ids if node in subgraph}

        nx.draw_networkx_labels(
            subgraph, pos,
            labels,
            font_size=8,
            font_weight='bold',
            ax=ax
        )

        ax.set_title('Actor Collaboration Network (Top 100 Actors)', fontsize=16, fontweight='bold', pad=20)
        ax.text(0.5, -0.05,
                f'Node size = degree centrality | Colors = communities | Edge width = collaboration frequency',
                ha='center', transform=ax.transAxes, fontsize=10, style='italic')
        ax.axis('off')

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'actor_network_graph.png', dpi=300, bbox_inches='tight')
        plt.close()

        print(f"✓ Saved: actor_network_graph.png")

    def visualize_degrees_of_separation(self):
        """Visualize degrees of separation distribution (Kevin Bacon analysis)."""
        print("Creating degrees of separation visualization...")

        if self.largest_component.number_of_nodes() == 0:
            print("! No network to analyze")
            return

        # Calculate shortest path lengths from most central actor
        most_central_actor = max(self.degree_centrality.items(), key=lambda x: x[1])[0]
        most_central_name = self.imdb_to_name.get(most_central_actor, 'Unknown')

        path_lengths = nx.single_source_shortest_path_length(
            self.largest_component,
            most_central_actor
        )

        # Count distribution
        separation_counts = Counter(path_lengths.values())

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

        # Bar chart
        degrees = sorted(separation_counts.keys())
        counts = [separation_counts[d] for d in degrees]

        ax1.bar(degrees, counts, color='steelblue', alpha=0.7, edgecolor='black')
        ax1.set_xlabel('Degrees of Separation', fontsize=12, fontweight='bold')
        ax1.set_ylabel('Number of Actors', fontsize=12, fontweight='bold')
        ax1.set_title(f'Degrees of Separation from {most_central_name}', fontsize=14, fontweight='bold')
        ax1.grid(axis='y', alpha=0.3)

        # Add value labels
        for i, (deg, count) in enumerate(zip(degrees, counts)):
            ax1.text(deg, count, f'{count:,}', ha='center', va='bottom', fontsize=9)

        # Cumulative distribution
        cumsum = np.cumsum(counts)
        cumsum_pct = (cumsum / cumsum[-1]) * 100

        ax2.plot(degrees, cumsum_pct, marker='o', linewidth=2, markersize=8, color='darkgreen')
        ax2.fill_between(degrees, cumsum_pct, alpha=0.3, color='green')
        ax2.set_xlabel('Degrees of Separation', fontsize=12, fontweight='bold')
        ax2.set_ylabel('Cumulative Percentage (%)', fontsize=12, fontweight='bold')
        ax2.set_title('Cumulative Reachability', fontsize=14, fontweight='bold')
        ax2.grid(alpha=0.3)
        ax2.set_ylim([0, 105])

        # Add percentage labels
        for deg, pct in zip(degrees, cumsum_pct):
            ax2.text(deg, pct + 2, f'{pct:.1f}%', ha='center', fontsize=9)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'degrees_of_separation_distribution.png', dpi=300, bbox_inches='tight')
        plt.close()

        # Store stats
        self.stats['max_separation'] = max(degrees)
        self.stats['avg_separation'] = np.mean(list(path_lengths.values()))
        self.stats['most_central_actor'] = most_central_name

        print(f"✓ Saved: degrees_of_separation_distribution.png")
        print(f"  Maximum separation: {self.stats['max_separation']} degrees")
        print(f"  Average separation: {self.stats['avg_separation']:.2f} degrees")

    def visualize_top_connected_actors(self):
        """Visualize top connected actors by centrality."""
        print("Creating top connected actors visualization...")

        if not self.degree_centrality:
            print("! No centrality data")
            return

        # Get top 20 by degree centrality
        top_20 = sorted(
            self.degree_centrality.items(),
            key=lambda x: x[1],
            reverse=True
        )[:20]

        actor_names = [self.imdb_to_name.get(actor_id, 'Unknown') for actor_id, _ in top_20]
        centrality_values = [cent for _, cent in top_20]

        fig, ax = plt.subplots(figsize=(12, 10))

        colors = plt.cm.viridis(np.linspace(0.3, 0.9, len(actor_names)))
        bars = ax.barh(range(len(actor_names)), centrality_values, color=colors, edgecolor='black')

        ax.set_yticks(range(len(actor_names)))
        ax.set_yticklabels(actor_names)
        ax.invert_yaxis()
        ax.set_xlabel('Degree Centrality', fontsize=12, fontweight='bold')
        ax.set_title('Top 20 Most Connected Actors', fontsize=14, fontweight='bold', pad=20)
        ax.grid(axis='x', alpha=0.3)

        # Add value labels
        for i, (bar, val) in enumerate(zip(bars, centrality_values)):
            ax.text(val, i, f' {val:.4f}', va='center', fontsize=9)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'top_connected_actors.png', dpi=300, bbox_inches='tight')
        plt.close()

        print(f"✓ Saved: top_connected_actors.png")

    def visualize_collaboration_frequency_heatmap(self):
        """Visualize top collaboration pairs as heatmap."""
        print("Creating collaboration frequency heatmap...")

        if self.collaboration_graph.number_of_edges() == 0:
            print("! No collaborations to visualize")
            return

        # Get top 30 most collaborative actors
        top_actors = sorted(
            self.degree_centrality.items(),
            key=lambda x: x[1],
            reverse=True
        )[:30]

        top_actor_ids = [actor_id for actor_id, _ in top_actors]

        # Build collaboration matrix
        matrix = np.zeros((len(top_actor_ids), len(top_actor_ids)))

        for i, actor1 in enumerate(top_actor_ids):
            for j, actor2 in enumerate(top_actor_ids):
                if self.collaboration_graph.has_edge(actor1, actor2):
                    matrix[i, j] = self.collaboration_graph[actor1][actor2]['weight']

        # Create heatmap
        fig, ax = plt.subplots(figsize=(16, 14))

        actor_names = [self.imdb_to_name.get(actor_id, 'Unknown')[:20] for actor_id in top_actor_ids]

        sns.heatmap(
            matrix,
            xticklabels=actor_names,
            yticklabels=actor_names,
            cmap='YlOrRd',
            annot=False,
            fmt='g',
            cbar_kws={'label': 'Number of Collaborations'},
            ax=ax
        )

        ax.set_title('Collaboration Frequency Heatmap (Top 30 Actors)', fontsize=14, fontweight='bold', pad=20)
        plt.xticks(rotation=45, ha='right', fontsize=8)
        plt.yticks(rotation=0, fontsize=8)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'collaboration_frequency_heatmap.png', dpi=300, bbox_inches='tight')
        plt.close()

        print(f"✓ Saved: collaboration_frequency_heatmap.png")

    def visualize_director_actor_network(self):
        """Visualize director-actor collaboration network."""
        print("Creating director-actor network visualization...")

        if self.director_actor_graph.number_of_edges() == 0:
            print("! No director-actor collaborations")
            return

        # Get top directors and actors by degree
        degrees = dict(self.director_actor_graph.degree())
        top_nodes = sorted(degrees.items(), key=lambda x: x[1], reverse=True)[:100]
        top_node_ids = [node_id for node_id, _ in top_nodes]

        subgraph = self.director_actor_graph.subgraph(top_node_ids)

        fig, ax = plt.subplots(figsize=(16, 16))

        # Node sizes based on degree
        node_sizes = [degrees[node] * 100 for node in subgraph.nodes()]

        # Node colors: distinguish directors vs actors (this is simplified)
        # In reality, some people are both. Use degree as proxy.
        node_colors = [degrees[node] for node in subgraph.nodes()]

        # Layout
        pos = nx.spring_layout(subgraph, k=2, iterations=50, seed=42)

        # Draw
        nx.draw_networkx_edges(
            subgraph, pos,
            alpha=0.2,
            edge_color='gray',
            ax=ax
        )

        nx.draw_networkx_nodes(
            subgraph, pos,
            node_size=node_sizes,
            node_color=node_colors,
            cmap='coolwarm',
            alpha=0.7,
            ax=ax
        )

        # Label top 15
        top_15_ids = [node_id for node_id, _ in top_nodes[:15]]
        labels = {node: self.imdb_to_name.get(node, 'Unknown')
                  for node in top_15_ids if node in subgraph}

        nx.draw_networkx_labels(
            subgraph, pos,
            labels,
            font_size=8,
            font_weight='bold',
            ax=ax
        )

        ax.set_title('Director-Actor Collaboration Network (Top 100)', fontsize=16, fontweight='bold', pad=20)
        ax.text(0.5, -0.05,
                'Node size = number of collaborations | Warmer colors = higher degree',
                ha='center', transform=ax.transAxes, fontsize=10, style='italic')
        ax.axis('off')

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'director_actor_network.png', dpi=300, bbox_inches='tight')
        plt.close()

        print(f"✓ Saved: director_actor_network.png")

    def visualize_network_communities(self):
        """Visualize detected communities in the network."""
        print("Creating network communities visualization...")

        if not self.communities:
            print("! No communities detected")
            return

        # Get top 150 actors for visualization
        top_actors = sorted(
            self.degree_centrality.items(),
            key=lambda x: x[1],
            reverse=True
        )[:150]

        top_actor_ids = [actor_id for actor_id, _ in top_actors]
        subgraph = self.largest_component.subgraph(top_actor_ids)

        fig, ax = plt.subplots(figsize=(18, 18))

        # Node colors by community
        node_colors = [self.communities[node] for node in subgraph.nodes()]

        # Node sizes by centrality
        node_sizes = [self.degree_centrality[node] * 3000 for node in subgraph.nodes()]

        # Layout
        pos = nx.spring_layout(subgraph, k=2.5, iterations=50, seed=42)

        # Draw
        nx.draw_networkx_edges(
            subgraph, pos,
            alpha=0.15,
            edge_color='gray',
            ax=ax
        )

        nx.draw_networkx_nodes(
            subgraph, pos,
            node_size=node_sizes,
            node_color=node_colors,
            cmap='tab20',
            alpha=0.8,
            ax=ax
        )

        # Label top 25 from each major community
        community_counts = Counter(node_colors)
        top_communities = [comm for comm, _ in community_counts.most_common(5)]

        labels_to_draw = {}
        for comm in top_communities:
            comm_actors = [actor_id for actor_id in subgraph.nodes()
                          if self.communities[actor_id] == comm]
            # Get top 5 from this community
            comm_top = sorted(comm_actors,
                            key=lambda x: self.degree_centrality[x],
                            reverse=True)[:5]
            for actor_id in comm_top:
                labels_to_draw[actor_id] = self.imdb_to_name.get(actor_id, 'Unknown')

        nx.draw_networkx_labels(
            subgraph, pos,
            labels_to_draw,
            font_size=7,
            font_weight='bold',
            ax=ax
        )

        ax.set_title('Network Communities (Top 150 Actors)', fontsize=16, fontweight='bold', pad=20)
        ax.text(0.5, -0.05,
                f'{self.stats["num_communities"]} communities detected | Colors = community membership',
                ha='center', transform=ax.transAxes, fontsize=10, style='italic')
        ax.axis('off')

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'network_communities.png', dpi=300, bbox_inches='tight')
        plt.close()

        print(f"✓ Saved: network_communities.png")

    def visualize_clustering_coefficient(self):
        """Visualize clustering coefficient distribution."""
        print("Creating clustering coefficient visualization...")

        if not self.clustering:
            print("! No clustering data")
            return

        clustering_values = list(self.clustering.values())

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

        # Histogram
        ax1.hist(clustering_values, bins=50, color='teal', alpha=0.7, edgecolor='black')
        ax1.axvline(self.stats['avg_clustering'], color='red', linestyle='--',
                   linewidth=2, label=f'Mean: {self.stats["avg_clustering"]:.4f}')
        ax1.set_xlabel('Clustering Coefficient', fontsize=12, fontweight='bold')
        ax1.set_ylabel('Frequency', fontsize=12, fontweight='bold')
        ax1.set_title('Clustering Coefficient Distribution', fontsize=14, fontweight='bold')
        ax1.legend()
        ax1.grid(axis='y', alpha=0.3)

        # Box plot
        ax2.boxplot(clustering_values, vert=True, patch_artist=True,
                   boxprops=dict(facecolor='lightblue', alpha=0.7),
                   medianprops=dict(color='red', linewidth=2))
        ax2.set_ylabel('Clustering Coefficient', fontsize=12, fontweight='bold')
        ax2.set_title('Clustering Coefficient Box Plot', fontsize=14, fontweight='bold')
        ax2.grid(axis='y', alpha=0.3)

        # Add stats
        stats_text = f'Mean: {np.mean(clustering_values):.4f}\n'
        stats_text += f'Median: {np.median(clustering_values):.4f}\n'
        stats_text += f'Std: {np.std(clustering_values):.4f}'
        ax2.text(0.98, 0.98, stats_text, transform=ax2.transAxes,
                verticalalignment='top', horizontalalignment='right',
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5),
                fontsize=10)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'clustering_coefficient_distribution.png', dpi=300, bbox_inches='tight')
        plt.close()

        print(f"✓ Saved: clustering_coefficient_distribution.png")

    def visualize_betweenness_centrality(self):
        """Visualize betweenness centrality (bridge actors)."""
        print("Creating betweenness centrality visualization...")

        if not self.betweenness_centrality:
            print("! No betweenness data")
            return

        # Get top 20 by betweenness
        top_20 = sorted(
            self.betweenness_centrality.items(),
            key=lambda x: x[1],
            reverse=True
        )[:20]

        actor_names = [self.imdb_to_name.get(actor_id, 'Unknown') for actor_id, _ in top_20]
        betweenness_values = [bet for _, bet in top_20]

        fig, ax = plt.subplots(figsize=(12, 10))

        colors = plt.cm.plasma(np.linspace(0.3, 0.9, len(actor_names)))
        bars = ax.barh(range(len(actor_names)), betweenness_values, color=colors, edgecolor='black')

        ax.set_yticks(range(len(actor_names)))
        ax.set_yticklabels(actor_names)
        ax.invert_yaxis()
        ax.set_xlabel('Betweenness Centrality', fontsize=12, fontweight='bold')
        ax.set_title('Top 20 Bridge Actors (Betweenness Centrality)', fontsize=14, fontweight='bold', pad=20)
        ax.text(0.5, -0.08,
                'Betweenness measures how often an actor lies on shortest paths between other actors',
                ha='center', transform=ax.transAxes, fontsize=10, style='italic')
        ax.grid(axis='x', alpha=0.3)

        # Add value labels
        for i, (bar, val) in enumerate(zip(bars, betweenness_values)):
            ax.text(val, i, f' {val:.4f}', va='center', fontsize=9)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'betweenness_centrality.png', dpi=300, bbox_inches='tight')
        plt.close()

        print(f"✓ Saved: betweenness_centrality.png")

    def visualize_collaboration_strength_vs_rating(self):
        """Visualize if frequent collaboration correlates with movie quality."""
        print("Creating collaboration strength vs rating visualization...")

        if self.collaboration_graph.number_of_edges() == 0:
            print("! No collaboration data")
            return

        # Extract edge data
        collaboration_counts = []
        avg_ratings = []

        for u, v, data in self.collaboration_graph.edges(data=True):
            count = data['weight']
            rating = data.get('avg_rating', 0)
            if rating > 0:  # Only include rated movies
                collaboration_counts.append(count)
                avg_ratings.append(rating)

        if not collaboration_counts:
            print("! No rated collaborations")
            return

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

        # Scatter plot
        ax1.scatter(collaboration_counts, avg_ratings, alpha=0.5, s=30, color='steelblue')
        ax1.set_xlabel('Number of Collaborations', fontsize=12, fontweight='bold')
        ax1.set_ylabel('Average Movie Rating', fontsize=12, fontweight='bold')
        ax1.set_title('Collaboration Frequency vs Movie Quality', fontsize=14, fontweight='bold')
        ax1.grid(alpha=0.3)

        # Add trendline
        z = np.polyfit(collaboration_counts, avg_ratings, 1)
        p = np.poly1d(z)
        ax1.plot(sorted(collaboration_counts), p(sorted(collaboration_counts)),
                "r--", alpha=0.8, linewidth=2, label=f'Trend: y={z[0]:.3f}x+{z[1]:.2f}')
        ax1.legend()

        # Calculate correlation
        from scipy.stats import pearsonr
        corr, p_value = pearsonr(collaboration_counts, avg_ratings)

        ax1.text(0.05, 0.95, f'Correlation: {corr:.3f}\np-value: {p_value:.4f}',
                transform=ax1.transAxes, verticalalignment='top',
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5),
                fontsize=10)

        # Box plot by collaboration count bins
        collab_bins = pd.cut(collaboration_counts, bins=[0, 1, 2, 3, 5, 10, 100],
                            labels=['1', '2', '3', '4-5', '6-10', '10+'])
        rating_by_bin = pd.DataFrame({
            'collaborations': collab_bins,
            'rating': avg_ratings
        })

        rating_by_bin.boxplot(column='rating', by='collaborations', ax=ax2,
                             patch_artist=True)
        ax2.set_xlabel('Number of Collaborations', fontsize=12, fontweight='bold')
        ax2.set_ylabel('Average Movie Rating', fontsize=12, fontweight='bold')
        ax2.set_title('Rating Distribution by Collaboration Frequency', fontsize=14, fontweight='bold')
        plt.suptitle('')  # Remove default title
        ax2.grid(axis='y', alpha=0.3)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'collaboration_strength_vs_rating.png', dpi=300, bbox_inches='tight')
        plt.close()

        self.stats['collaboration_rating_correlation'] = corr
        self.stats['collaboration_rating_pvalue'] = p_value

        print(f"✓ Saved: collaboration_strength_vs_rating.png")
        print(f"  Correlation: {corr:.3f} (p={p_value:.4f})")

    def visualize_network_density_over_time(self):
        """Visualize how collaboration networks evolve over time."""
        print("Creating network density over time visualization...")

        # Build network by decade
        decade_networks = defaultdict(lambda: nx.Graph())

        for u, v, data in self.collaboration_graph.edges(data=True):
            movies = data['movies']
            for movie in movies:
                year = movie.get('year', '')
                if year and not pd.isna(year):
                    try:
                        year_int = int(float(year))
                        decade = (year_int // 10) * 10
                        if 1900 <= decade <= 2020:
                            decade_networks[decade].add_edge(u, v)
                    except:
                        continue

        if not decade_networks:
            print("! No temporal data available")
            return

        # Calculate metrics by decade
        decades = sorted(decade_networks.keys())
        num_actors = []
        num_collaborations = []
        densities = []
        avg_degrees = []

        for decade in decades:
            G = decade_networks[decade]
            num_actors.append(G.number_of_nodes())
            num_collaborations.append(G.number_of_edges())
            densities.append(nx.density(G) if G.number_of_nodes() > 1 else 0)
            avg_degrees.append(np.mean([d for n, d in G.degree()]) if G.number_of_nodes() > 0 else 0)

        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))

        # Number of actors
        ax1.plot(decades, num_actors, marker='o', linewidth=2, markersize=8, color='steelblue')
        ax1.fill_between(decades, num_actors, alpha=0.3, color='steelblue')
        ax1.set_xlabel('Decade', fontsize=12, fontweight='bold')
        ax1.set_ylabel('Number of Actors', fontsize=12, fontweight='bold')
        ax1.set_title('Actors in Network by Decade', fontsize=14, fontweight='bold')
        ax1.grid(alpha=0.3)

        # Number of collaborations
        ax2.plot(decades, num_collaborations, marker='s', linewidth=2, markersize=8, color='darkgreen')
        ax2.fill_between(decades, num_collaborations, alpha=0.3, color='green')
        ax2.set_xlabel('Decade', fontsize=12, fontweight='bold')
        ax2.set_ylabel('Number of Collaborations', fontsize=12, fontweight='bold')
        ax2.set_title('Collaborations by Decade', fontsize=14, fontweight='bold')
        ax2.grid(alpha=0.3)

        # Network density
        ax3.plot(decades, densities, marker='^', linewidth=2, markersize=8, color='darkred')
        ax3.fill_between(decades, densities, alpha=0.3, color='red')
        ax3.set_xlabel('Decade', fontsize=12, fontweight='bold')
        ax3.set_ylabel('Network Density', fontsize=12, fontweight='bold')
        ax3.set_title('Network Density by Decade', fontsize=14, fontweight='bold')
        ax3.grid(alpha=0.3)

        # Average degree
        ax4.plot(decades, avg_degrees, marker='D', linewidth=2, markersize=8, color='purple')
        ax4.fill_between(decades, avg_degrees, alpha=0.3, color='purple')
        ax4.set_xlabel('Decade', fontsize=12, fontweight='bold')
        ax4.set_ylabel('Average Degree', fontsize=12, fontweight='bold')
        ax4.set_title('Average Connections per Actor by Decade', fontsize=14, fontweight='bold')
        ax4.grid(alpha=0.3)

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'network_density_over_time.png', dpi=300, bbox_inches='tight')
        plt.close()

        print(f"✓ Saved: network_density_over_time.png")

    def visualize_top_collaborating_pairs(self):
        """Visualize the most frequent collaboration pairs."""
        print("Creating top collaborating pairs visualization...")

        if self.collaboration_graph.number_of_edges() == 0:
            print("! No collaborations")
            return

        # Get top 20 pairs by collaboration count
        edges_with_weight = [
            (u, v, data['weight'])
            for u, v, data in self.collaboration_graph.edges(data=True)
        ]

        top_20_edges = sorted(edges_with_weight, key=lambda x: x[2], reverse=True)[:20]

        pair_labels = []
        weights = []

        for u, v, weight in top_20_edges:
            name1 = self.imdb_to_name.get(u, 'Unknown')
            name2 = self.imdb_to_name.get(v, 'Unknown')
            pair_labels.append(f"{name1} ↔ {name2}")
            weights.append(weight)

        fig, ax = plt.subplots(figsize=(12, 10))

        colors = plt.cm.Reds(np.linspace(0.4, 0.9, len(pair_labels)))
        bars = ax.barh(range(len(pair_labels)), weights, color=colors, edgecolor='black')

        ax.set_yticks(range(len(pair_labels)))
        ax.set_yticklabels(pair_labels, fontsize=9)
        ax.invert_yaxis()
        ax.set_xlabel('Number of Collaborations', fontsize=12, fontweight='bold')
        ax.set_title('Top 20 Most Frequent Collaboration Pairs', fontsize=14, fontweight='bold', pad=20)
        ax.grid(axis='x', alpha=0.3)

        # Add value labels
        for i, (bar, val) in enumerate(zip(bars, weights)):
            ax.text(val, i, f' {val}', va='center', fontsize=9, fontweight='bold')

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'top_collaborating_pairs.png', dpi=300, bbox_inches='tight')
        plt.close()

        print(f"✓ Saved: top_collaborating_pairs.png")

    def visualize_ego_network(self):
        """Visualize ego network of the most connected actor."""
        print("Creating ego network visualization...")

        if not self.degree_centrality:
            print("! No centrality data")
            return

        # Get most central actor
        most_central = max(self.degree_centrality.items(), key=lambda x: x[1])[0]
        most_central_name = self.imdb_to_name.get(most_central, 'Unknown')

        # Get ego network (actor + immediate connections)
        ego_graph = nx.ego_graph(self.largest_component, most_central, radius=1)

        fig, ax = plt.subplots(figsize=(16, 16))

        # Node colors: central actor vs neighbors
        node_colors = ['red' if node == most_central else 'lightblue'
                      for node in ego_graph.nodes()]

        # Node sizes: central actor larger
        node_sizes = [5000 if node == most_central else 1000
                     for node in ego_graph.nodes()]

        # Edge widths
        edge_widths = [ego_graph[u][v]['weight'] * 0.5
                      for u, v in ego_graph.edges()]

        # Layout
        pos = nx.spring_layout(ego_graph, k=3, iterations=50, seed=42)

        # Draw
        nx.draw_networkx_edges(
            ego_graph, pos,
            width=edge_widths,
            alpha=0.3,
            edge_color='gray',
            ax=ax
        )

        nx.draw_networkx_nodes(
            ego_graph, pos,
            node_size=node_sizes,
            node_color=node_colors,
            alpha=0.8,
            edgecolors='black',
            linewidths=2,
            ax=ax
        )

        # Labels for all nodes
        labels = {node: self.imdb_to_name.get(node, 'Unknown')
                 for node in ego_graph.nodes()}

        nx.draw_networkx_labels(
            ego_graph, pos,
            labels,
            font_size=7,
            font_weight='bold',
            ax=ax
        )

        ax.set_title(f'Ego Network: {most_central_name} (Red) and Direct Collaborators',
                    fontsize=16, fontweight='bold', pad=20)
        ax.text(0.5, -0.05,
                f'{ego_graph.number_of_nodes()-1} direct collaborators | Edge width = collaboration frequency',
                ha='center', transform=ax.transAxes, fontsize=10, style='italic')
        ax.axis('off')

        plt.tight_layout()
        plt.savefig(self.viz_dir / 'ego_network_top_actor.png', dpi=300, bbox_inches='tight')
        plt.close()

        print(f"✓ Saved: ego_network_top_actor.png")

    def visualize_interactive_network(self):
        """Create interactive network visualization using plotly."""
        print("Creating interactive network visualization...")

        if self.largest_component.number_of_nodes() == 0:
            print("! No network to visualize")
            return

        # Get top 200 actors for interactive visualization
        top_actors = sorted(
            self.degree_centrality.items(),
            key=lambda x: x[1],
            reverse=True
        )[:200]

        top_actor_ids = [actor_id for actor_id, _ in top_actors]
        subgraph = self.largest_component.subgraph(top_actor_ids)

        # Create layout
        pos = nx.spring_layout(subgraph, k=2, iterations=50, seed=42)

        # Create edge trace
        edge_x = []
        edge_y = []
        edge_weights = []

        for edge in subgraph.edges():
            x0, y0 = pos[edge[0]]
            x1, y1 = pos[edge[1]]
            edge_x.append(x0)
            edge_x.append(x1)
            edge_x.append(None)
            edge_y.append(y0)
            edge_y.append(y1)
            edge_y.append(None)
            edge_weights.append(subgraph[edge[0]][edge[1]]['weight'])

        edge_trace = go.Scatter(
            x=edge_x, y=edge_y,
            line=dict(width=0.5, color='#888'),
            hoverinfo='none',
            mode='lines')

        # Create node trace
        node_x = []
        node_y = []
        node_text = []
        node_size = []
        node_color = []

        for node in subgraph.nodes():
            x, y = pos[node]
            node_x.append(x)
            node_y.append(y)

            name = self.imdb_to_name.get(node, 'Unknown')
            degree = self.degree_centrality[node]
            community = self.communities.get(node, 0)

            # Get collaborations count
            collaborations = subgraph.degree(node)

            node_text.append(f"{name}<br>Degree: {degree:.4f}<br>Collaborations: {collaborations}<br>Community: {community}")
            node_size.append(degree * 100)
            node_color.append(community)

        node_trace = go.Scatter(
            x=node_x, y=node_y,
            mode='markers+text',
            hoverinfo='text',
            text=node_text,
            marker=dict(
                showscale=True,
                colorscale='Viridis',
                size=node_size,
                color=node_color,
                colorbar=dict(
                    thickness=15,
                    title='Community',
                    xanchor='left',
                    titleside='right'
                ),
                line=dict(width=2, color='white')))

        # Create figure
        fig = go.Figure(data=[edge_trace, node_trace],
                       layout=go.Layout(
                           title=dict(
                               text='Interactive Actor Collaboration Network (Top 200 Actors)',
                               x=0.5,
                               xanchor='center'
                           ),
                           titlefont_size=16,
                           showlegend=False,
                           hovermode='closest',
                           margin=dict(b=20, l=5, r=5, t=40),
                           annotations=[dict(
                               text="Node size = degree centrality | Color = community | Hover for details",
                               showarrow=False,
                               xref="paper", yref="paper",
                               x=0.5, y=-0.05,
                               xanchor='center',
                               font=dict(size=10)
                           )],
                           xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                           yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                           width=1400,
                           height=1000
                       ))

        # Save as HTML
        fig.write_html(str(self.viz_dir / 'interactive_network.html'))
        print(f"✓ Saved: interactive_network.html")

    def generate_report(self):
        """Generate comprehensive text report."""
        print("Generating report...")

        report_path = self.reports_dir / 'batch_16_collaboration_networks_report.txt'

        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("="*80 + "\n")
            f.write("CINESCOPE BATCH 16: COLLABORATION NETWORKS ANALYSIS\n")
            f.write("="*80 + "\n\n")

            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

            f.write(f"Total Movies Processed: {self.stats.get('movies_processed', 0):,}\n")
            f.write(f"Total People in Dataset: {len(self.people_cache):,}\n\n")

            f.write("="*80 + "\n")
            f.write("ACTOR COLLABORATION NETWORK\n")
            f.write("="*80 + "\n\n")

            f.write(f"Total Actors in Network: {self.stats.get('total_actors', 0):,}\n")
            f.write(f"Total Collaborations: {self.stats.get('total_collaborations', 0):,}\n")
            f.write(f"Network Density: {self.stats.get('network_density', 0):.6f}\n")
            f.write(f"Largest Connected Component: {self.stats.get('largest_component_size', 0):,} actors\n\n")

            if 'most_central_actor' in self.stats:
                f.write(f"Most Central Actor: {self.stats['most_central_actor']}\n")
                f.write(f"Maximum Degrees of Separation: {self.stats.get('max_separation', 0)}\n")
                f.write(f"Average Degrees of Separation: {self.stats.get('avg_separation', 0):.2f}\n\n")

            f.write("="*80 + "\n")
            f.write("NETWORK STRUCTURE\n")
            f.write("="*80 + "\n\n")

            f.write(f"Number of Communities Detected: {self.stats.get('num_communities', 0)}\n")
            f.write(f"Average Clustering Coefficient: {self.stats.get('avg_clustering', 0):.4f}\n\n")

            if 'collaboration_rating_correlation' in self.stats:
                f.write("="*80 + "\n")
                f.write("COLLABORATION QUALITY ANALYSIS\n")
                f.write("="*80 + "\n\n")

                corr = self.stats['collaboration_rating_correlation']
                pval = self.stats['collaboration_rating_pvalue']

                f.write(f"Correlation (Collaboration Frequency vs Rating): {corr:.4f}\n")
                f.write(f"P-value: {pval:.6f}\n")

                if pval < 0.05:
                    if corr > 0:
                        f.write(f"Result: Significant POSITIVE correlation - frequent collaborators make better films\n")
                    else:
                        f.write(f"Result: Significant NEGATIVE correlation - frequent collaborators make worse films\n")
                else:
                    f.write(f"Result: No significant correlation between collaboration frequency and film quality\n")

                f.write("\n")

            f.write("="*80 + "\n")
            f.write("TOP 10 MOST CONNECTED ACTORS\n")
            f.write("="*80 + "\n\n")

            if self.degree_centrality:
                top_10 = sorted(self.degree_centrality.items(), key=lambda x: x[1], reverse=True)[:10]
                for i, (actor_id, centrality) in enumerate(top_10, 1):
                    name = self.imdb_to_name.get(actor_id, 'Unknown')
                    f.write(f"{i:4}. {name:50} {centrality:.6f}\n")

            f.write("\n")

            f.write("="*80 + "\n")
            f.write("END OF REPORT\n")
            f.write("="*80 + "\n")

        print(f"✓ Report saved: {report_path}")
        print()

    def run(self):
        """Run the complete analysis pipeline."""
        try:
            self.load_data()
            self.build_collaboration_networks()
            self.calculate_network_metrics()

            print("\nGenerating visualizations...")
            print("-" * 80)

            self.visualize_actor_network()
            self.visualize_degrees_of_separation()
            self.visualize_top_connected_actors()
            self.visualize_collaboration_frequency_heatmap()
            self.visualize_director_actor_network()
            self.visualize_network_communities()
            self.visualize_clustering_coefficient()
            self.visualize_betweenness_centrality()
            self.visualize_collaboration_strength_vs_rating()
            self.visualize_network_density_over_time()
            self.visualize_top_collaborating_pairs()
            self.visualize_ego_network()
            self.visualize_interactive_network()

            print("-" * 80)
            print()

            self.generate_report()

            print("="*80)
            print("BATCH 16 ANALYSIS COMPLETE!")
            print("="*80)
            print(f"\nVisualizations saved to: {self.viz_dir}")
            print(f"Report saved to: {self.reports_dir}")
            print()

        except Exception as e:
            print(f"\n❌ Error during analysis: {str(e)}")
            import traceback
            traceback.print_exc()
            raise

if __name__ == '__main__':
    analyzer = CollaborationNetworkAnalyzer()
    analyzer.run()
