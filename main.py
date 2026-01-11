from src.pipeline import run_pipeline

if __name__ == "__main__":
    if run_pipeline():
        print("Pipeline executado com sucesso.")
    else:
        print("Falha na execução do pipeline.")