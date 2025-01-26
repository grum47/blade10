import subprocess
import logging
from typing import List

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RcloneManager:
    def __init__(self, config_path: str):
        self.config_path = config_path

    def run_command(self, command: List[str]) -> None:
        """
        Запуск команды rclone.
        """
        logger.info(f"Running command: {' '.join(command)}")
        result = subprocess.run(command, capture_output=True, text=True)
        if result.returncode != 0:
            logger.error(f"Command failed with error: {result.stderr.strip()}")
            raise RuntimeError(f"Rclone command failed: {result.stderr.strip()}")
        logger.info("Command executed successfully.")
    
    def sync_data(self, source: str, destination: str) -> None:
        """
        Синхронизация данных между источником и назначением.
        """
        command = ['rclone', 'sync', source, destination, '--config', self.config_path]
        self.run_command(command)

    def copy_data(self, source: str, destination: str) -> None:
        """
        Копирование данных между источником и назначением.
        """
        command = ['rclone', 'copy', source, destination, '--config', self.config_path]
        self.run_command(command)

    def delete_data(self, path: str) -> None:
        """
        Удаление данных по указанному пути.
        """
        command = ['rclone', 'delete', path, '--config', self.config_path]
        self.run_command(command)

    def list_data(self, remote: str) -> List[str]:
        """
        Список данных в указанном удаленном хранилище.
        """
        command = ['rclone', 'ls', remote, '--config', self.config_path]
        result = subprocess.run(command, capture_output=True, text=True)

        if result.returncode != 0:
            logger.error(f"Listing command failed with error: {result.stderr.strip()}")
            raise RuntimeError(f"Rclone list command failed: {result.stderr.strip()}")

        logger.info("Data listed successfully.")
        return result.stdout.splitlines()
