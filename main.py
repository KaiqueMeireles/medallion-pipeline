import sys

from src.pipeline import run_pipeline

if __name__ == "__main__":
    try:
        success = run_pipeline()
        if success:
            print("Pipeline executado com sucesso.")
        else:
            print("Falha na execução do pipeline.")
    except Exception as exc:
        print(f"Erro durante a execução do pipeline: {exc}")
        sys.exit(1)