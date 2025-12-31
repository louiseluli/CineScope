#!/usr/bin/env python3
"""
CineScope Batch 31: Network Analysis
====================================

Actor collaboration network analysis using graph theory.
Analyzes co-appearance patterns, centrality, and community structure.

Author: CineScope Analytics  
Date: 2025-12-31
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import networkx as nx
from pathlib import Path
from datetime import datetime
import ast
from collections import Counter, defaultdict
from scipy import stats

try:
    import community as community_louvain
    HAS_COMMUNITY = True
except ImportError:
    HAS_COMMUNITY = False
    print("Warning: python-louvain not available, community detection disabled")

class NetworkAnalyzer:
    """Analyze actor collaboration networks from film cast data."""
    
    def __init__(self, data_path='data/processed/watched_movies_master.csv'):
        """Initialize analyzer with watched movies dataset."""
        self.data_path = Path(data_path)
        self.output_dir = Path('analysis_outputs/visualizations/batch_31')
        self.report_dir = Path('analysis_outputs/reports')
        
        # Create output directories
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.report_dir.mkdir(parents=True, exist_ok=True)
        
        # Load data
        print("Loading watched movies data...")
        self.df = pd.read_csv(self.data_path)
        print(f"Loaded {len(self.df)} watched films")
        
        # Build network
        print("Building actor collaboration network...")
        self.G = nx.Graph()
        self.actor_films = defaultdict(list)
        self.film_actors = {}
        self._build_network()
        
        print(f"Network: {self.G.number_of_nodes()} actors, {self.G.number_of_edges()} connections")
        
        # Set up plotting
        plt.style.use('default')
        sns.set_palette("husl")
    
    def _build_network(self):
        """Build actor collaboration network from cast data."""
        for idx, row in self.df.iterrows():
            if pd.isna(row['tmdb_cast']):
                continue
            
            try:
                cast = ast.literal_eval(row['tmdb_cast'])
                if not isinstance(cast, list) or len(cast) == 0:
                    continue
                
                # Get actor names (top 10 billed)
                actors = [actor['name'] for actor in cast[:10] if 'name' in actor]
                
                if len(actors) < 2:
                    continue
                
                film_title = row['title']
                self.film_actors[film_title] = actors
                
                # Record which films each actor appeared in
                for actor in actors:
                    self.actor_films[actor].append(film_title)
                
                # Add edges between all pairs of actors in this film
                for i, actor1 in enumerate(actors):
                    for actor2 in actors[i+1:]:
                        if self.G.has_edge(actor1, actor2):
                            # Increment weight
                            self.G[actor1][actor2]['weight'] += 1
                            self.G[actor1][actor2]['films'].append(film_title)
                        else:
                            # Create new edge
                            self.G.add_edge(actor1, actor2, weight=1, films=[film_title])
            
            except Exception as e:
                continue
    
    def visualize_network_overview(self):
        """Visualization 1-3: Network metrics and most connected actors."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Actor Network Overview', fontsize=16, fontweight='bold')
        
        # 1. Degree distribution
        ax1 = axes[0, 0]
        degrees = [d for n, d in self.G.degree()]
        
        if degrees:
            ax1.hist(degrees, bins=50, color='skyblue', edgecolor='black', alpha=0.7)
            ax1.set_xlabel('Degree (Number of Connections)', fontsize=11, fontweight='bold')
            ax1.set_ylabel('Number of Actors', fontsize=11, fontweight='bold')
            ax1.set_title(f'Degree Distribution ({len(degrees)} actors)', fontsize=12, fontweight='bold')
            ax1.grid(axis='y', alpha=0.3)
            
            mean_degree = np.mean(degrees)
            ax1.axvline(mean_degree, color='red', linestyle='--', linewidth=2,
                       label=f'Mean: {mean_degree:.1f}')
            ax1.legend()
        
        # 2. Most connected actors
        ax2 = axes[0, 1]
        degree_dict = dict(self.G.degree())
        top_actors = sorted(degree_dict.items(), key=lambda x: x[1], reverse=True)[:15]
        
        if top_actors:
            actors, degrees = zip(*top_actors)
            y_pos = np.arange(len(actors))
            
            bars = ax2.barh(y_pos, degrees, color=plt.cm.Spectral(np.linspace(0, 1, len(actors))))
            ax2.set_yticks(y_pos)
            ax2.set_yticklabels([a[:25] + '...' if len(a) > 25 else a for a in actors], fontsize=9)
            ax2.set_xlabel('Number of Connections', fontsize=11, fontweight='bold')
            ax2.set_title('Most Connected Actors', fontsize=12, fontweight='bold')
            ax2.invert_yaxis()
            ax2.grid(axis='x', alpha=0.3)
            
            # Add film counts
            for i, actor in enumerate(actors):
                film_count = len(self.actor_films[actor])
                ax2.text(degrees[i] + 1, i, f'({film_count} films)',
                        va='center', fontsize=8)
        
        # 3. Network metrics
        ax3 = axes[1, 0]
        ax3.axis('off')
        
        # Calculate metrics
        metrics_text = "NETWORK METRICS\n" + "="*40 + "\n\n"
        metrics_text += f"Nodes (Actors): {self.G.number_of_nodes():,}\n"
        metrics_text += f"Edges (Connections): {self.G.number_of_edges():,}\n\n"
        
        if self.G.number_of_nodes() > 0:
            density = nx.density(self.G)
            metrics_text += f"Network Density: {density:.4f}\n"
            
            avg_degree = np.mean([d for n, d in self.G.degree()])
            metrics_text += f"Average Degree: {avg_degree:.2f}\n\n"
            
            # Connected components
            components = list(nx.connected_components(self.G))
            metrics_text += f"Connected Components: {len(components)}\n"
            
            if components:
                largest = max(components, key=len)
                metrics_text += f"Largest Component: {len(largest):,} actors\n"
                metrics_text += f"({len(largest)/self.G.number_of_nodes()*100:.1f}% of network)\n\n"
            
            # Clustering
            try:
                avg_clustering = nx.average_clustering(self.G)
                metrics_text += f"Average Clustering: {avg_clustering:.4f}\n"
            except:
                pass
        
        ax3.text(0.1, 0.9, metrics_text, transform=ax3.transAxes,
                fontsize=11, verticalalignment='top',
                family='monospace',
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
        ax3.set_title('Network Statistics', fontsize=12, fontweight='bold')
        
        # 4. Collaboration strength distribution
        ax4 = axes[1, 1]
        weights = [data['weight'] for u, v, data in self.G.edges(data=True)]
        
        if weights:
            weight_counts = Counter(weights)
            collabs = sorted(weight_counts.items())
            counts_x, counts_y = zip(*collabs)
            
            ax4.bar(counts_x, counts_y, color='#E74C3C', edgecolor='black')
            ax4.set_xlabel('Films Together', fontsize=11, fontweight='bold')
            ax4.set_ylabel('Number of Actor Pairs', fontsize=11, fontweight='bold')
            ax4.set_title('Collaboration Strength Distribution', fontsize=12, fontweight='bold')
            ax4.grid(axis='y', alpha=0.3)
            
            # Annotate
            total_pairs = len(weights)
            multi_film = sum(1 for w in weights if w > 1)
            ax4.text(0.95, 0.95, f'Total pairs: {total_pairs:,}\nMultiple films: {multi_film:,}',
                    transform=ax4.transAxes, fontsize=9, verticalalignment='top',
                    horizontalalignment='right',
                    bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
        
        plt.tight_layout()
        output_path = self.output_dir / 'network_overview.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved: {output_path}")
    
    def visualize_centrality(self):
        """Visualization 4-6: Centrality measures."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Actor Centrality Analysis', fontsize=16, fontweight='bold')
        
        # Calculate centrality measures
        print("  Calculating centrality metrics...")
        
        # 1. Betweenness centrality
        ax1 = axes[0, 0]
        try:
            betweenness = nx.betweenness_centrality(self.G)
            top_between = sorted(betweenness.items(), key=lambda x: x[1], reverse=True)[:12]
            
            if top_between:
                actors, scores = zip(*top_between)
                y_pos = np.arange(len(actors))
                
                bars = ax1.barh(y_pos, scores, color='#3498DB', edgecolor='black')
                ax1.set_yticks(y_pos)
                ax1.set_yticklabels([a[:25] + '...' if len(a) > 25 else a for a in actors], fontsize=9)
                ax1.set_xlabel('Betweenness Centrality', fontsize=11, fontweight='bold')
                ax1.set_title('Bridge Actors (Betweenness)', fontsize=12, fontweight='bold')
                ax1.invert_yaxis()
                ax1.grid(axis='x', alpha=0.3)
        except:
            ax1.text(0.5, 0.5, 'Network too large for betweenness calculation',
                    ha='center', va='center', transform=ax1.transAxes)
        
        # 2. Closeness centrality  
        ax2 = axes[0, 1]
        try:
            # Use largest component for closeness
            components = list(nx.connected_components(self.G))
            if components:
                largest_component = self.G.subgraph(max(components, key=len))
                closeness = nx.closeness_centrality(largest_component)
                top_close = sorted(closeness.items(), key=lambda x: x[1], reverse=True)[:12]
                
                if top_close:
                    actors, scores = zip(*top_close)
                    y_pos = np.arange(len(actors))
                    
                    bars = ax2.barh(y_pos, scores, color='#2ECC71', edgecolor='black')
                    ax2.set_yticks(y_pos)
                    ax2.set_yticklabels([a[:25] + '...' if len(a) > 25 else a for a in actors], fontsize=9)
                    ax2.set_xlabel('Closeness Centrality', fontsize=11, fontweight='bold')
                    ax2.set_title('Central Actors (Closeness)', fontsize=12, fontweight='bold')
                    ax2.invert_yaxis()
                    ax2.grid(axis='x', alpha=0.3)
        except:
            ax2.text(0.5, 0.5, 'Closeness calculation failed',
                    ha='center', va='center', transform=ax2.transAxes)
        
        # 3. Eigenvector centrality
        ax3 = axes[1, 0]
        try:
            eigenvector = nx.eigenvector_centrality(self.G, max_iter=100)
            top_eigen = sorted(eigenvector.items(), key=lambda x: x[1], reverse=True)[:12]
            
            if top_eigen:
                actors, scores = zip(*top_eigen)
                y_pos = np.arange(len(actors))
                
                bars = ax3.barh(y_pos, scores, color='#F39C12', edgecolor='black')
                ax3.set_yticks(y_pos)
                ax3.set_yticklabels([a[:25] + '...' if len(a) > 25 else a for a in actors], fontsize=9)
                ax3.set_xlabel('Eigenvector Centrality', fontsize=11, fontweight='bold')
                ax3.set_title('Influential Actors (Eigenvector)', fontsize=12, fontweight='bold')
                ax3.invert_yaxis()
                ax3.grid(axis='x', alpha=0.3)
        except:
            ax3.text(0.5, 0.5, 'Eigenvector calculation failed',
                    ha='center', va='center', transform=ax3.transAxes)
        
        # 4. Centrality correlation
        ax4 = axes[1, 1]
        try:
            degree_cent = dict(self.G.degree())
            
            if len(betweenness) > 0 and len(degree_cent) > 0:
                # Get common actors
                common = set(betweenness.keys()) & set(degree_cent.keys())
                if len(common) > 10:
                    degrees = [degree_cent[a] for a in common]
                    between_vals = [betweenness[a] for a in common]
                    
                    ax4.scatter(degrees, between_vals, alpha=0.6, s=50, edgecolors='black')
                    ax4.set_xlabel('Degree Centrality', fontsize=11, fontweight='bold')
                    ax4.set_ylabel('Betweenness Centrality', fontsize=11, fontweight='bold')
                    ax4.set_title('Centrality Correlation', fontsize=12, fontweight='bold')
                    ax4.grid(True, alpha=0.3)
                    
                    # Add correlation
                    if len(degrees) >= 2:
                        corr, p_val = stats.pearsonr(degrees, between_vals)
                        ax4.text(0.05, 0.95, f'Correlation: {corr:.3f}\np-value: {p_val:.4f}',
                                transform=ax4.transAxes, fontsize=10, verticalalignment='top',
                                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
        except:
            pass
        
        plt.tight_layout()
        output_path = self.output_dir / 'centrality_analysis.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved: {output_path}")
    def visualize_communities(self):
        """Visualization 7-9: Community detection and analysis."""
        if not HAS_COMMUNITY:
            print("Skipping community detection (python-louvain not available)")
            return
        
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Actor Community Analysis', fontsize=16, fontweight='bold')
        
        # Get largest component for community detection
        components = list(nx.connected_components(self.G))
        if not components:
            print("No connected components found")
            return
        
        largest_component = self.G.subgraph(max(components, key=len)).copy()
        
        print("  Detecting communities...")
        
        # 1. Community detection
        ax1 = axes[0, 0]
        try:
            partition = community_louvain.best_partition(largest_component)
            
            # Count communities
            communities = {}
            for node, comm_id in partition.items():
                if comm_id not in communities:
                    communities[comm_id] = []
                communities[comm_id].append(node)
            
            # Community size distribution
            sizes = [len(members) for members in communities.values()]
            
            ax1.hist(sizes, bins=30, color='purple', edgecolor='black', alpha=0.7)
            ax1.set_xlabel('Community Size', fontsize=11, fontweight='bold')
            ax1.set_ylabel('Number of Communities', fontsize=11, fontweight='bold')
            ax1.set_title(f'Community Size Distribution ({len(communities)} communities)', 
                         fontsize=12, fontweight='bold')
            ax1.grid(axis='y', alpha=0.3)
            
            # Stats
            avg_size = np.mean(sizes)
            median_size = np.median(sizes)
            ax1.axvline(avg_size, color='red', linestyle='--', linewidth=2,
                       label=f'Mean: {avg_size:.1f}')
            ax1.axvline(median_size, color='blue', linestyle='--', linewidth=2,
                       label=f'Median: {median_size:.1f}')
            ax1.legend()
            
        except Exception as e:
            ax1.text(0.5, 0.5, f'Community detection failed: {str(e)}',
                    ha='center', va='center', transform=ax1.transAxes)
        
        # 2. Largest communities
        ax2 = axes[0, 1]
        try:
            if 'communities' in locals():
                # Sort by size
                sorted_communities = sorted(communities.items(), 
                                          key=lambda x: len(x[1]), reverse=True)[:10]
                
                comm_labels = [f'Community {comm_id}' for comm_id, _ in sorted_communities]
                comm_sizes = [len(members) for _, members in sorted_communities]
                
                y_pos = np.arange(len(comm_labels))
                bars = ax2.barh(y_pos, comm_sizes, 
                              color=plt.cm.tab10(np.linspace(0, 1, len(comm_labels))))
                ax2.set_yticks(y_pos)
                ax2.set_yticklabels(comm_labels, fontsize=9)
                ax2.set_xlabel('Number of Actors', fontsize=11, fontweight='bold')
                ax2.set_title('Largest Communities', fontsize=12, fontweight='bold')
                ax2.invert_yaxis()
                ax2.grid(axis='x', alpha=0.3)
                
                # Add top actor from each community
                for i, (comm_id, members) in enumerate(sorted_communities):
                    # Get most connected actor in this community
                    subgraph = largest_component.subgraph(members)
                    degrees = dict(subgraph.degree())
                    if degrees:
                        top_actor = max(degrees, key=degrees.get)
                        ax2.text(comm_sizes[i] + 2, i, 
                               f'{top_actor[:20]}...' if len(top_actor) > 20 else top_actor,
                               va='center', fontsize=8)
        except:
            pass
        
        # 3. Modularity score
        ax3 = axes[1, 0]
        ax3.axis('off')
        
        try:
            if 'partition' in locals():
                modularity = community_louvain.modularity(partition, largest_component)
                
                metrics_text = "COMMUNITY METRICS\n" + "="*40 + "\n\n"
                metrics_text += f"Number of Communities: {len(communities)}\n"
                metrics_text += f"Modularity Score: {modularity:.4f}\n\n"
                metrics_text += f"Largest Component: {largest_component.number_of_nodes()} actors\n"
                metrics_text += f"Average Community Size: {np.mean(sizes):.1f}\n"
                metrics_text += f"Median Community Size: {np.median(sizes):.1f}\n"
                metrics_text += f"Largest Community: {max(sizes)} actors\n"
                metrics_text += f"Smallest Community: {min(sizes)} actors\n\n"
                
                # Top 3 communities
                metrics_text += "TOP 3 COMMUNITIES:\n"
                for i, (comm_id, members) in enumerate(sorted_communities[:3], 1):
                    subgraph = largest_component.subgraph(members)
                    degrees = dict(subgraph.degree())
                    top_actor = max(degrees, key=degrees.get) if degrees else "N/A"
                    metrics_text += f"{i}. Community {comm_id}: {len(members)} actors\n"
                    metrics_text += f"   Hub: {top_actor[:30]}...\n" if len(top_actor) > 30 else f"   Hub: {top_actor}\n"
                
                ax3.text(0.1, 0.9, metrics_text, transform=ax3.transAxes,
                        fontsize=10, verticalalignment='top',
                        family='monospace',
                        bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.5))
                ax3.set_title('Community Statistics', fontsize=12, fontweight='bold')
        except:
            pass
        
        # 4. Community connectivity
        ax4 = axes[1, 1]
        try:
            if 'communities' in locals() and len(communities) > 1:
                # Calculate inter-community edges
                top_comms = sorted_communities[:8]  # Top 8 communities
                comm_dict = {comm_id: set(members) for comm_id, members in top_comms}
                
                # Build connectivity matrix
                n = len(top_comms)
                connectivity = np.zeros((n, n))
                
                for i, (comm_id_i, members_i) in enumerate(top_comms):
                    for j, (comm_id_j, members_j) in enumerate(top_comms):
                        if i != j:
                            # Count edges between communities
                            edge_count = 0
                            for actor_i in members_i:
                                for actor_j in members_j:
                                    if largest_component.has_edge(actor_i, actor_j):
                                        edge_count += 1
                            connectivity[i, j] = edge_count
                
                # Plot heatmap
                im = ax4.imshow(connectivity, cmap='YlOrRd', aspect='auto')
                ax4.set_xticks(np.arange(n))
                ax4.set_yticks(np.arange(n))
                ax4.set_xticklabels([f'C{comm_id}' for comm_id, _ in top_comms])
                ax4.set_yticklabels([f'C{comm_id}' for comm_id, _ in top_comms])
                ax4.set_title('Inter-Community Connectivity', fontsize=12, fontweight='bold')
                
                # Add colorbar
                plt.colorbar(im, ax=ax4, label='Number of Connections')
                
                # Annotate cells
                for i in range(n):
                    for j in range(n):
                        if connectivity[i, j] > 0:
                            text = ax4.text(j, i, int(connectivity[i, j]),
                                          ha="center", va="center", color="black", fontsize=8)
        except:
            pass
        
        plt.tight_layout()
        output_path = self.output_dir / 'community_analysis.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved: {output_path}")
    
    def visualize_frequent_costars(self):
        """Visualization 10: Most frequent co-star pairs."""
        fig, axes = plt.subplots(1, 2, figsize=(16, 8))
        fig.suptitle('Frequent Co-Star Collaborations', fontsize=16, fontweight='bold')
        
        # 1. Top co-star pairs
        ax1 = axes[0]
        
        # Get all edges with weights
        edges_with_weights = [(u, v, data['weight'], data['films']) 
                              for u, v, data in self.G.edges(data=True)]
        
        # Sort by weight
        top_pairs = sorted(edges_with_weights, key=lambda x: x[2], reverse=True)[:15]
        
        if top_pairs:
            labels = []
            weights = []
            
            for actor1, actor2, weight, films in top_pairs:
                # Truncate names
                name1 = actor1[:15] + '...' if len(actor1) > 15 else actor1
                name2 = actor2[:15] + '...' if len(actor2) > 15 else actor2
                labels.append(f'{name1}\n& {name2}')
                weights.append(weight)
            
            y_pos = np.arange(len(labels))
            bars = ax1.barh(y_pos, weights, color=plt.cm.viridis(np.linspace(0, 1, len(labels))))
            ax1.set_yticks(y_pos)
            ax1.set_yticklabels(labels, fontsize=9)
            ax1.set_xlabel('Films Together', fontsize=11, fontweight='bold')
            ax1.set_title('Most Frequent Co-Star Pairs', fontsize=12, fontweight='bold')
            ax1.invert_yaxis()
            ax1.grid(axis='x', alpha=0.3)
            
            # Annotate with film count
            for i, weight in enumerate(weights):
                ax1.text(weight + 0.1, i, f'{weight} films', va='center', fontsize=8)
        
        # 2. Sample films for top pair
        ax2 = axes[1]
        ax2.axis('off')
        
        if top_pairs:
            actor1, actor2, weight, films = top_pairs[0]
            
            sample_text = f"TOP CO-STAR PAIR\n" + "="*50 + "\n\n"
            sample_text += f"{actor1}\n& {actor2}\n\n"
            sample_text += f"Films Together: {weight}\n\n"
            sample_text += "SAMPLE COLLABORATIONS:\n"
            sample_text += "-" * 50 + "\n"
            
            # Show up to 10 films
            for i, film in enumerate(films[:10], 1):
                sample_text += f"{i}. {film}\n"
            
            if len(films) > 10:
                sample_text += f"\n... and {len(films) - 10} more films"
            
            ax2.text(0.1, 0.9, sample_text, transform=ax2.transAxes,
                    fontsize=10, verticalalignment='top',
                    family='monospace',
                    bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.5))
            ax2.set_title('Most Frequent Collaboration Details', fontsize=12, fontweight='bold')
        
        plt.tight_layout()
        output_path = self.output_dir / 'frequent_costars.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved: {output_path}")
    
    def visualize_network_evolution(self):
        """Visualization 11: Network growth over time."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Network Evolution Over Time', fontsize=16, fontweight='bold')
        
        # Extract year information
        print("  Analyzing network evolution by decade...")
        
        # Build decade-based networks
        decade_stats = defaultdict(lambda: {'nodes': set(), 'edges': 0, 'films': 0})
        
        for idx, row in self.df.iterrows():
            if pd.isna(row.get('year')) or pd.isna(row['tmdb_cast']):
                continue
            
            try:
                year = int(row['year'])
                decade = (year // 10) * 10
                
                cast = ast.literal_eval(row['tmdb_cast'])
                actors = [actor['name'] for actor in cast[:10] if 'name' in actor]
                
                if len(actors) >= 2:
                    decade_stats[decade]['films'] += 1
                    decade_stats[decade]['nodes'].update(actors)
                    
                    # Count potential edges
                    num_edges = len(actors) * (len(actors) - 1) // 2
                    decade_stats[decade]['edges'] += num_edges
            except:
                continue
        
        # Sort by decade
        sorted_decades = sorted(decade_stats.items())
        
        if sorted_decades:
            decades = [d for d, _ in sorted_decades]
            nodes_count = [len(stats['nodes']) for _, stats in sorted_decades]
            edges_count = [stats['edges'] for _, stats in sorted_decades]
            films_count = [stats['films'] for _, stats in sorted_decades]
            
            # 1. Network size growth
            ax1 = axes[0, 0]
            ax1.plot(decades, nodes_count, marker='o', linewidth=2, markersize=8, 
                    color='#3498DB', label='Unique Actors')
            ax1.set_xlabel('Decade', fontsize=11, fontweight='bold')
            ax1.set_ylabel('Number of Actors', fontsize=11, fontweight='bold')
            ax1.set_title('Actor Network Growth', fontsize=12, fontweight='bold')
            ax1.grid(True, alpha=0.3)
            ax1.legend()
            
            # 2. Connections growth
            ax2 = axes[0, 1]
            ax2.plot(decades, edges_count, marker='s', linewidth=2, markersize=8,
                    color='#E74C3C', label='Total Connections')
            ax2.set_xlabel('Decade', fontsize=11, fontweight='bold')
            ax2.set_ylabel('Number of Connections', fontsize=11, fontweight='bold')
            ax2.set_title('Connection Growth', fontsize=12, fontweight='bold')
            ax2.grid(True, alpha=0.3)
            ax2.legend()
            
            # 3. Films per decade
            ax3 = axes[1, 0]
            bars = ax3.bar(decades, films_count, width=8, color='#2ECC71', edgecolor='black')
            ax3.set_xlabel('Decade', fontsize=11, fontweight='bold')
            ax3.set_ylabel('Number of Films', fontsize=11, fontweight='bold')
            ax3.set_title('Films per Decade', fontsize=12, fontweight='bold')
            ax3.grid(axis='y', alpha=0.3)
            
            # Annotate bars
            for bar, count in zip(bars, films_count):
                height = bar.get_height()
                ax3.text(bar.get_x() + bar.get_width()/2., height,
                        f'{count}', ha='center', va='bottom', fontsize=8)
            
            # 4. Cumulative growth
            ax4 = axes[1, 1]
            cumulative_nodes = np.cumsum(nodes_count)
            cumulative_edges = np.cumsum(edges_count)
            
            ax4_twin = ax4.twinx()
            
            line1 = ax4.plot(decades, cumulative_nodes, marker='o', linewidth=2, 
                           markersize=8, color='#9B59B6', label='Cumulative Actors')
            line2 = ax4_twin.plot(decades, cumulative_edges, marker='s', linewidth=2,
                                markersize=8, color='#F39C12', label='Cumulative Connections')
            
            ax4.set_xlabel('Decade', fontsize=11, fontweight='bold')
            ax4.set_ylabel('Cumulative Actors', fontsize=11, fontweight='bold', color='#9B59B6')
            ax4_twin.set_ylabel('Cumulative Connections', fontsize=11, fontweight='bold', color='#F39C12')
            ax4.set_title('Cumulative Network Growth', fontsize=12, fontweight='bold')
            ax4.grid(True, alpha=0.3)
            
            # Combine legends
            lines = line1 + line2
            labels = [l.get_label() for l in lines]
            ax4.legend(lines, labels, loc='upper left')
        
        plt.tight_layout()
        output_path = self.output_dir / 'network_evolution.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved: {output_path}")
    
    def generate_report(self):
        """Generate comprehensive text report."""
        print("\nGenerating comprehensive report...")
        
        report_lines = []
        report_lines.append("=" * 80)
        report_lines.append("CINESCOPE BATCH 31: NETWORK ANALYSIS")
        report_lines.append("=" * 80)
        report_lines.append("")
        report_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append("")
        
        # Overall statistics
        report_lines.append("=" * 80)
        report_lines.append("OVERALL STATISTICS")
        report_lines.append("=" * 80)
        report_lines.append("")
        report_lines.append(f"Total Films: {len(self.df)}")
        report_lines.append(f"Films with Cast Data: {len(self.film_actors)}")
        report_lines.append("")
        report_lines.append(f"Network Size: {self.G.number_of_nodes():,} actors")
        report_lines.append(f"Total Connections: {self.G.number_of_edges():,}")
        report_lines.append("")
        
        # Network metrics
        if self.G.number_of_nodes() > 0:
            density = nx.density(self.G)
            avg_degree = np.mean([d for n, d in self.G.degree()])
            
            report_lines.append(f"Network Density: {density:.6f}")
            report_lines.append(f"Average Degree: {avg_degree:.2f}")
            report_lines.append("")
            
            # Connected components
            components = list(nx.connected_components(self.G))
            report_lines.append(f"Connected Components: {len(components)}")
            
            if components:
                largest = max(components, key=len)
                report_lines.append(f"Largest Component: {len(largest):,} actors ({len(largest)/self.G.number_of_nodes()*100:.1f}%)")
            report_lines.append("")
            
            # Clustering
            try:
                avg_clustering = nx.average_clustering(self.G)
                report_lines.append(f"Average Clustering Coefficient: {avg_clustering:.4f}")
            except:
                pass
        
        # Most connected actors
        report_lines.append("")
        report_lines.append("=" * 80)
        report_lines.append("MOST CONNECTED ACTORS")
        report_lines.append("=" * 80)
        report_lines.append("")
        
        degree_dict = dict(self.G.degree())
        top_actors = sorted(degree_dict.items(), key=lambda x: x[1], reverse=True)[:20]
        
        for i, (actor, degree) in enumerate(top_actors, 1):
            film_count = len(self.actor_films[actor])
            report_lines.append(f"{i:2d}. {actor:40s} - {degree:4d} connections ({film_count} films)")
        
        # Frequent co-stars
        report_lines.append("")
        report_lines.append("=" * 80)
        report_lines.append("MOST FREQUENT CO-STAR PAIRS")
        report_lines.append("=" * 80)
        report_lines.append("")
        
        edges_with_weights = [(u, v, data['weight'], data['films']) 
                              for u, v, data in self.G.edges(data=True)]
        top_pairs = sorted(edges_with_weights, key=lambda x: x[2], reverse=True)[:15]
        
        for i, (actor1, actor2, weight, films) in enumerate(top_pairs, 1):
            report_lines.append(f"{i:2d}. {actor1} & {actor2}")
            report_lines.append(f"    Films together: {weight}")
            report_lines.append(f"    Sample: {', '.join(films[:3])}")
            if len(films) > 3:
                report_lines.append(f"    ... and {len(films) - 3} more")
            report_lines.append("")
        
        # Centrality measures
        report_lines.append("=" * 80)
        report_lines.append("CENTRALITY ANALYSIS")
        report_lines.append("=" * 80)
        report_lines.append("")
        
        # Betweenness
        try:
            print("  Calculating betweenness centrality for report...")
            betweenness = nx.betweenness_centrality(self.G)
            top_between = sorted(betweenness.items(), key=lambda x: x[1], reverse=True)[:10]
            
            report_lines.append("BRIDGE ACTORS (Betweenness Centrality):")
            report_lines.append("-" * 80)
            for i, (actor, score) in enumerate(top_between, 1):
                report_lines.append(f"{i:2d}. {actor:40s} - {score:.6f}")
            report_lines.append("")
        except:
            report_lines.append("Betweenness centrality calculation skipped (network too large)")
            report_lines.append("")
        
        # Eigenvector
        try:
            print("  Calculating eigenvector centrality for report...")
            eigenvector = nx.eigenvector_centrality(self.G, max_iter=100)
            top_eigen = sorted(eigenvector.items(), key=lambda x: x[1], reverse=True)[:10]
            
            report_lines.append("INFLUENTIAL ACTORS (Eigenvector Centrality):")
            report_lines.append("-" * 80)
            for i, (actor, score) in enumerate(top_eigen, 1):
                report_lines.append(f"{i:2d}. {actor:40s} - {score:.6f}")
            report_lines.append("")
        except:
            report_lines.append("Eigenvector centrality calculation skipped")
            report_lines.append("")
        
        # Actor filmography stats
        report_lines.append("=" * 80)
        report_lines.append("ACTOR FILMOGRAPHY")
        report_lines.append("=" * 80)
        report_lines.append("")
        
        film_counts = [len(films) for films in self.actor_films.values()]
        
        report_lines.append(f"Average films per actor: {np.mean(film_counts):.2f}")
        report_lines.append(f"Median films per actor: {np.median(film_counts):.0f}")
        report_lines.append(f"Max films per actor: {max(film_counts)}")
        report_lines.append("")
        
        # Most prolific actors
        top_prolific = sorted(self.actor_films.items(), key=lambda x: len(x[1]), reverse=True)[:15]
        
        report_lines.append("MOST PROLIFIC ACTORS:")
        report_lines.append("-" * 80)
        for i, (actor, films) in enumerate(top_prolific, 1):
            degree = degree_dict.get(actor, 0)
            report_lines.append(f"{i:2d}. {actor:40s} - {len(films):3d} films ({degree} connections)")
        
        report_lines.append("")
        report_lines.append("=" * 80)
        report_lines.append("END OF REPORT")
        report_lines.append("=" * 80)
        
        # Write report
        report_path = self.report_dir / 'batch_31_network_analysis_report.txt'
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(report_lines))
        
        print(f"Report saved: {report_path}")
    
    def run_all_analyses(self):
        """Execute all visualizations and generate report."""
        print("\n" + "="*80)
        print("BATCH 31: NETWORK ANALYSIS")
        print("="*80)
        
        print("\n[1/6] Creating network overview visualizations...")
        self.visualize_network_overview()
        
        print("\n[2/6] Creating centrality analysis...")
        self.visualize_centrality()
        
        print("\n[3/6] Analyzing communities...")
        self.visualize_communities()
        
        print("\n[4/6] Finding frequent co-stars...")
        self.visualize_frequent_costars()
        
        print("\n[5/6] Analyzing network evolution...")
        self.visualize_network_evolution()
        
        print("\n[6/6] Generating comprehensive report...")
        self.generate_report()
        
        print("\n" + "="*80)
        print("BATCH 31 COMPLETE")
        print("="*80)
        print(f"\nVisualizations saved to: {self.output_dir}")
        print(f"Report saved to: {self.report_dir}/batch_31_network_analysis_report.txt")


def main():
    """Main execution function."""
    analyzer = NetworkAnalyzer()
    analyzer.run_all_analyses()


if __name__ == '__main__':
    main()
