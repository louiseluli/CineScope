"""
Completeness Analysis Runner
Simplified script to build the database and run enhanced completeness analyses.
"""

import sys
import subprocess
from pathlib import Path
import argparse

def check_database_exists():
    """Check if completeness database exists."""
    db_path = Path('data/processed/completeness.db')
    return db_path.exists()

def build_database():
    """Build the completeness database."""
    print("\n" + "=" * 80)
    print("BUILDING COMPLETENESS DATABASE")
    print("=" * 80)
    print("\nThis will take 10-30 minutes on the first run...")
    print("Future analyses will be much faster using this database!\n")

    result = subprocess.run(
        [sys.executable, 'scripts/utils/build_completeness_database.py'],
        cwd=Path.cwd()
    )

    if result.returncode == 0:
        print("\n✓ Database built successfully!")
        return True
    else:
        print("\n✗ Database build failed!")
        return False

def run_actor_analysis():
    """Run actor completeness analysis."""
    print("\n" + "=" * 80)
    print("RUNNING ACTOR COMPLETENESS ANALYSIS")
    print("=" * 80)

    result = subprocess.run(
        [sys.executable, 'scripts/batch_34_actor_completeness.py'],
        cwd=Path.cwd()
    )

    if result.returncode == 0:
        print("\n✓ Actor analysis completed successfully!")
        return True
    else:
        print("\n✗ Actor analysis failed!")
        return False

def main():
    parser = argparse.ArgumentParser(
        description='Run completeness analyses',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Build database and run actor analysis
  python scripts/run_completeness_analysis.py --all

  # Only build database
  python scripts/run_completeness_analysis.py --build

  # Only run actor analysis (database must exist)
  python scripts/run_completeness_analysis.py --actor

  # Force rebuild database even if it exists
  python scripts/run_completeness_analysis.py --rebuild
        """
    )

    parser.add_argument('--build', action='store_true',
                       help='Build the completeness database')
    parser.add_argument('--rebuild', action='store_true',
                       help='Rebuild database even if it exists')
    parser.add_argument('--actor', action='store_true',
                       help='Run enhanced actor analysis')
    parser.add_argument('--all', action='store_true',
                       help='Build database (if needed) and run all analyses')

    args = parser.parse_args()

    # If no arguments, show help
    if not any([args.build, args.rebuild, args.actor, args.all]):
        parser.print_help()
        return

    db_exists = check_database_exists()

    # Handle rebuild
    if args.rebuild:
        print("\nRebuilding database (existing database will be replaced)...")
        if db_exists:
            db_path = Path('data/processed/completeness.db')
            db_path.unlink()
            print(f"✓ Deleted existing database: {db_path}")
        build_database()
        db_exists = True

    # Handle build
    if args.build and not db_exists:
        build_database()
        db_exists = True

    # Handle all
    if args.all:
        if not db_exists:
            print("\nDatabase not found. Building database first...")
            if not build_database():
                print("\n✗ Cannot proceed without database!")
                return
            db_exists = True

        print("\nRunning all analyses...")
        run_actor_analysis()

    # Handle individual analyses
    if args.actor:
        if not db_exists:
            print("\n✗ Database not found!")
            print("Please run with --build first to create the database.")
            print("\nExample: python scripts/run_completeness_analysis.py --build")
            return

        run_actor_analysis()

    print("\n" + "=" * 80)
    print("COMPLETENESS ANALYSIS RUNNER - COMPLETE")
    print("=" * 80)
    print("\nOutputs:")
    print("  • Visualizations: analysis_outputs/visualizations/batch_34/")
    print("  • Reports: analysis_outputs/reports/")
    print("  • Database: data/processed/completeness.db")

if __name__ == "__main__":
    main()
