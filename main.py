from modules.raw_layer import create_raw_layer
from modules.silver_layer import create_silver_layer
from modules.gold_layer import create_gold_layer
from utils.logger import logger

def main():
    try:
        logger.info('Creating raw layer')
        create_raw_layer()

        logger.info('Creating silver layer')
        create_silver_layer()

        logger.info('Creating gold layer')
        create_gold_layer()

        logger.info('Data pipeline completed successfully.')

    except Exception as e:
        logger.error(f'Data pipeline failed: {str(e)}')

if __name__ == '__main__':
    main()
