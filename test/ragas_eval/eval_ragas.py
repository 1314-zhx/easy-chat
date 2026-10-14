import json
import os
from pathlib import Path

import pandas as pd
from datasets import Dataset
from ragas import evaluate
from ragas.metrics.collections import AnswerRelevancy, Faithfulness, ContextPrecision, ContextRecall
from langchain_openai import ChatOpenAI, OpenAIEmbeddings

ROOT = Path(__file__).resolve().parent
SAMPLES = ROOT / "rag_eval_samples.json"
OUT = ROOT / "ragas_results.json"

OPENAI_BASE_URL = os.environ.get("OPENAI_BASE_URL", "https://dashscope.aliyuncs.com/compatible-mode/v1")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "qwen-plus")
EMBED_MODEL = os.environ.get("OPENAI_EMBED_MODEL", "text-embedding-v3")


def build_dataset(samples):
    rows = []
    for s in samples:
        gold = None
        for m in s["messages"]:
            if m["id"] == s["gold_message_id"]:
                gold = m
                break
        if gold is None:
            continue
        contexts = [m["content"] for m in s["messages"]]
        rows.append(
            {
                "question": s["query"],
                "answer": gold["content"],
                "contexts": contexts,
                "ground_truth": gold["content"],
            }
        )
    return Dataset.from_pandas(pd.DataFrame(rows))


def main():
    with open(SAMPLES, "r", encoding="utf-8") as f:
        samples = json.load(f)

    ds = build_dataset(samples)

    llm = ChatOpenAI(
        model=OPENAI_MODEL,
        api_key=OPENAI_API_KEY,
        base_url=OPENAI_BASE_URL,
        temperature=0,
    )
    embeddings = OpenAIEmbeddings(
        model=EMBED_MODEL,
        api_key=OPENAI_API_KEY,
        base_url=OPENAI_BASE_URL,
    )

    result = evaluate(
        ds,
        metrics=[
            ContextPrecision(llm=llm),
            ContextRecall(llm=llm),
            Faithfulness(llm=llm),
            AnswerRelevancy(llm=llm),
        ],
        llm=llm,
        embeddings=embeddings,
    )

    OUT.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "summary": result.to_pandas().describe(include="all").to_dict(),
        "rows": result.to_pandas().to_dict(orient="records"),
    }
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(result)
    print(f"saved to {OUT}")


if __name__ == "__main__":
    main()
