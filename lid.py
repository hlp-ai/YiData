import fasttext


class FastTextLID:

    def __init__(self, model_path="lid.176.bin"):
        self._model = fasttext.load_model(model_path)

    def lid(self, text, k=1):
        r = self._model.predict(text, k=k)
        langs = r[0]
        probs = r[1]
        return list(zip(langs, probs))


if __name__ == "__main__":
    lid = FastTextLID()
    print(lid.lid("this is a just test."))
    print(lid.lid("this is a just test. 这里包括中文。", k=2))
