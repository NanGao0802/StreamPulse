import logging
import os
from typing import Any, Dict, List

from dotenv import load_dotenv
from modelscope.pipelines import pipeline as ms_pipeline
from modelscope.utils.constant import Tasks

load_dotenv("/opt/pipeline/conf/alert.env")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


class StructBERTSentimentService:
    _instance = None

    def __init__(self) -> None:
        self.model_id = os.getenv(
            "STRUCTBERT_MODEL_ID",
            "iic/nlp_structbert_sentiment-classification_chinese-base"
        )
        self.batch_size = _env_int("STRUCTBERT_BATCH_SIZE", 32)
        self.neutral_threshold = _env_float("STRUCTBERT_NEUTRAL_THRESHOLD", 0.72)
        self.max_text_length = _env_int("STRUCTBERT_MAX_TEXT_LENGTH", 256)

        logger.info(f"Loading StructBERT model: {self.model_id}")
        self.pipe = ms_pipeline(task=Tasks.text_classification, model=self.model_id)
        logger.info("StructBERT model loaded")

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = StructBERTSentimentService()
        return cls._instance

    def _normalize_text(self, text: Any) -> str:
        if text is None:
            return ""
        s = str(text).strip().replace("\n", " ").replace("\r", " ")
        if len(s) > self.max_text_length:
            s = s[: self.max_text_length]
        return s

    def _canonical_label(self, label: Any) -> str:
        s = str(label).strip().lower()

        if s in {"1", "label_1", "positive", "pos"}:
            return "positive"
        if s in {"0", "label_0", "negative", "neg"}:
            return "negative"

        if "positive" in s or "pos" in s or "正" in s:
            return "positive"
        if "negative" in s or "neg" in s or "负" in s:
            return "negative"

        return "unknown"

    def _parse_output(self, output: Any) -> Dict[str, Any]:
        pos_prob = 0.0
        neg_prob = 0.0
        raw_label = "unknown"
        confidence = 0.0

        if isinstance(output, dict):
            labels = output.get("labels")
            scores = output.get("scores")

            if isinstance(labels, list) and isinstance(scores, list) and len(labels) == len(scores):
                for lbl, sc in zip(labels, scores):
                    canon = self._canonical_label(lbl)
                    score = float(sc)
                    if canon == "positive":
                        pos_prob = score
                    elif canon == "negative":
                        neg_prob = score

                if pos_prob >= neg_prob:
                    raw_label = "positive"
                    confidence = pos_prob
                else:
                    raw_label = "negative"
                    confidence = neg_prob

            elif "label" in output and "score" in output:
                raw_label = self._canonical_label(output["label"])
                confidence = float(output["score"])
                if raw_label == "positive":
                    pos_prob = confidence
                    neg_prob = 1.0 - confidence
                elif raw_label == "negative":
                    neg_prob = confidence
                    pos_prob = 1.0 - confidence

        score_value = round(pos_prob - neg_prob, 4)

        if confidence < self.neutral_threshold:
            final_label = "neutral"
        else:
            final_label = raw_label if raw_label in {"positive", "negative"} else "neutral"

        if final_label == "neutral":
            score_value = 0.0

        return {
            "sentiment_raw_label": raw_label,
            "sentiment_confidence": round(float(confidence), 4),
            "sentiment_label": final_label,
            "sentiment_score": score_value,
            "sentiment_source": "structbert",
            "sentiment_model": self.model_id,
        }

    def _predict_chunk(self, texts: List[str]) -> List[Dict[str, Any]]:
        if not texts:
            return []

        try:
            result = self.pipe(texts)
            if isinstance(result, list) and len(result) == len(texts):
                return [self._parse_output(x) for x in result]
            if isinstance(result, dict) and len(texts) == 1:
                return [self._parse_output(result)]
        except Exception as e:
            logger.warning(f"Batch inference failed, fallback to single inference. reason={e}")

        outputs = []
        for text in texts:
            try:
                result = self.pipe(text)
                outputs.append(self._parse_output(result))
            except Exception as e:
                logger.exception(f"Single inference failed, fallback neutral. reason={e}")
                outputs.append({
                    "sentiment_raw_label": "unknown",
                    "sentiment_confidence": 0.0,
                    "sentiment_label": "neutral",
                    "sentiment_score": 0.0,
                    "sentiment_source": "structbert",
                    "sentiment_model": self.model_id,
                })
        return outputs

    def predict_texts(self, texts: List[Any]) -> List[Dict[str, Any]]:
        clean_texts = [self._normalize_text(t) for t in texts]
        results: List[Dict[str, Any]] = []

        for i in range(0, len(clean_texts), self.batch_size):
            chunk = clean_texts[i:i + self.batch_size]
            chunk_result = self._predict_chunk(chunk)
            results.extend(chunk_result)

        return results
