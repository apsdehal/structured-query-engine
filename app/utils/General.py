def getAnalyzer(analyzer_type="standard"):
    analyzer_type = analyzer_type.lower()
    if analyzer_type == "standard":
        from app.helpers.analyzers import StandardAnalyzer
        analyzer = StandardAnalyzer.StandardAnalyzer()

    return analyzer
