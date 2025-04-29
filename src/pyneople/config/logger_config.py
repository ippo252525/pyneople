import logging

def setup_logging():
    """
    메인에서 로그 설정 하는 함수
    """
    root_logger = logging.getLogger()
    
    # 이미 핸들러가 설정되어 있지 않은 경우에만 설정
    if not root_logger.handlers:  
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )