from structbert_sentiment import StructBERTSentimentService

svc = StructBERTSentimentService.get_instance()

texts = [
    "数学到底要怎么学呢，我学的太迷茫了",
    "老师讲得很好，我感觉进步很大",
    "今天正常复习，没什么特别的感觉"
]

results = svc.predict_texts(texts)

for text, result in zip(texts, results):
    print("=" * 80)
    print("text:", text)
    print("result:", result)
