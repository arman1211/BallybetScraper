import multiprocessing as mp
import signal
import sys
import time
from pathlib import Path
import logging


# Worker functions must be at module level for Windows multiprocessing
def run_pregame_worker():
    """Pregame scraper worker function."""
    from ballybet_scraper import BallyBetScraper
    
    scraper = BallyBetScraper(data_dir="ballybet_data")
    while True:
        try:
            scraper.run(max_workers=5)
            time.sleep(300)  # Wait 5 minutes between full scrapes
        except KeyboardInterrupt:
            break
        except Exception as e:
            logging.error(f"Error in pregame scraper: {e}")
            time.sleep(60)


def run_live_worker():
    """Live scraper worker function."""
    from ballybet_live_scraper import LiveBallyBetScraper
    
    scraper = LiveBallyBetScraper(
        data_dir="ballybet_data",
        poll_interval=60
    )
    scraper.run()


def run_status_worker():
    """Status updater worker function."""
    from game_status_updater import GameStatusUpdater
    
    updater = GameStatusUpdater(
        update_interval=5,
        starting_soon_minutes=30
    )
    updater.run()


class WorkerCoordinator:
    """
    Coordinates and manages multiple scraper workers running in parallel.
    Handles graceful shutdown and monitors worker health.
    """
    
    def __init__(self):
        self.processes = {}
        self.running = False
        self.setup_logging()
        
        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
    
    def setup_logging(self):
        """Setup coordinator logging."""
        log_dir = Path("ballybet_data/logs")
        log_dir.mkdir(parents=True, exist_ok=True)
        
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - [COORDINATOR] - %(message)s",
            handlers=[
                logging.FileHandler(log_dir / "coordinator.log", encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        self.logger.info(f"Received signal {signum}. Initiating graceful shutdown...")
        self.stop_all_workers()
        sys.exit(0)
    
    def start_pregame_scraper(self):
        """Start the pregame scraper worker."""
        try:
            process = mp.Process(target=run_pregame_worker, name="PregameScraper")
            process.start()
            self.processes['pregame'] = process
            self.logger.info("✅ Pregame scraper worker started")
        except Exception as e:
            self.logger.error(f"Failed to start pregame scraper: {e}", exc_info=True)
    
    def start_live_scraper(self):
        """Start the live scraper worker."""
        try:
            process = mp.Process(target=run_live_worker, name="LiveScraper")
            process.start()
            self.processes['live'] = process
            self.logger.info("✅ Live scraper worker started")
        except Exception as e:
            self.logger.error(f"Failed to start live scraper: {e}", exc_info=True)
    
    def start_status_updater(self):
        """Start the status updater worker."""
        try:
            process = mp.Process(target=run_status_worker, name="StatusUpdater")
            process.start()
            self.processes['status'] = process
            self.logger.info("✅ Status updater worker started")
        except Exception as e:
            self.logger.error(f"Failed to start status updater: {e}", exc_info=True)
    
    def monitor_workers(self):
        """Monitor worker health and restart if needed."""
        while self.running:
            for name, process in list(self.processes.items()):
                if not process.is_alive():
                    self.logger.warning(f"⚠️ Worker '{name}' died. Restarting...")
                    
                    # Restart based on worker type
                    if name == 'pregame':
                        self.start_pregame_scraper()
                    elif name == 'live':
                        self.start_live_scraper()
                    elif name == 'status':
                        self.start_status_updater()
            
            time.sleep(10)  # Check every 10 seconds
    
    def stop_all_workers(self):
        """Stop all worker processes gracefully."""
        self.running = False
        self.logger.info("Stopping all workers...")
        
        for name, process in self.processes.items():
            if process.is_alive():
                self.logger.info(f"Terminating {name} worker...")
                process.terminate()
                process.join(timeout=10)
                
                if process.is_alive():
                    self.logger.warning(f"Force killing {name} worker...")
                    process.kill()
                    process.join()
        
        self.logger.info("All workers stopped")
    
    def run(self):
        """Start all workers and monitor them."""
        self.logger.info("=" * 60)
        self.logger.info("🚀 Starting BallyyBet Multi-Worker System")
        self.logger.info("=" * 60)
        
        self.running = True
        
        # Start all workers
        self.start_pregame_scraper()
        time.sleep(2)  # Stagger starts
        
        self.start_live_scraper()
        time.sleep(2)
        
        self.start_status_updater()
        
        self.logger.info("=" * 60)
        self.logger.info("✅ All workers started successfully")
        self.logger.info("=" * 60)
        self.logger.info("Press Ctrl+C to stop all workers")
        self.logger.info("")
        
        # Monitor workers
        try:
            self.monitor_workers()
        except KeyboardInterrupt:
            self.logger.info("Received keyboard interrupt")
        finally:
            self.stop_all_workers()


def main():
    """Main entry point."""
    # Set multiprocessing start method for Windows compatibility
    if sys.platform == 'win32':
        mp.set_start_method('spawn', force=True)
    
    coordinator = WorkerCoordinator()
    coordinator.run()


if __name__ == "__main__":
    main()