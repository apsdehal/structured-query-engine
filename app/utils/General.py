import app.helpers.analyzers as analyzers


def getAnalyzer(analyzer_type="standard"):
    analyzer_type = analyzer_type.lower()
    if analyzer_type == "standard":
        analyzer = analyzers.StandardAnalyzer.StandardAnalyzer()

    return analyzer
