from vaex.dataframe import DataFrame


def to_code(df: DataFrame):
    """Convert a vaex :class:`DataFrame` object to a python code snippet.

    With this you can do `print(to_code(df))` to get a code snippet that can be
    copy pasted into a python file to recreate the same dataframe.

    :param df: :class:`DataFrame` object to convert.
    :return: Python code snippet as a string.
    """
    recs = df.to_records()
    cols = list(recs[0].keys())
    vals = [tuple(rec.values()) for rec in recs]
    code = f"""
columns = {cols}
records = {vals}
serieses = zip(*records)
arrs = dict(zip(columns, serieses))
df = vaex.from_arrays(**arrs)
"""
    return code
