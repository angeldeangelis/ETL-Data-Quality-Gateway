"""
main.py
System Ignition Component - Production Gateway Runner.
"""

import logging
from pathlib import Path
from data_gateway.engine import EnterpriseETLEngine
from data_gateway.strategies.financial import FinancialValidationStrategy # Importación del dialecto
from data_gateway.exceptions import DataGatewayException

# Configuración del Logger Corporativo
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

def main():
    workspace = Path(__file__).parent
    
    # 1. Materializar el Contrato y el Motor en la RAM
    financial_law = FinancialValidationStrategy()
    engine = EnterpriseETLEngine(base_dir=workspace, strategy=financial_law)
    
    # 2. Descubrimiento de archivos
    payloads = engine.discover_payloads()
    print(f"\n🚀 System Active. Discovered {len(payloads)} streams to process.\n")
    
    # 3. Bucle de ejecución resiliente
    for stream in payloads:
        try:
            engine.process_stream(stream)
        except DataGatewayException as error:
            # Captura quirúrgica de nuestras alertas customizadas
            print(f"⚠️ Isolated System Exception handled successfully: {error}")
        except Exception as unexpected:
            print(f"🚨 Unhandled infrastructure collapse: {unexpected}")

if __name__ == "__main__":
    main()