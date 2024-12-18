
from .cowsay import say_dicts, get_dict_texts, say

def test_say_dicts():
    d = {'a': 'b', 'c': 'd', "e": 1, "x": "foo"}
    keys = {"a", "c", "x"}
    
    result = say_dicts([d], keys)
    assert "b d foo" in result

def test_get_dict_texts():
    d = {'a': 'b', 'c': 'd', "e": 1, "x": "foo"}
    keys = {"a", "c", "x"}

    result = get_dict_texts(d, keys)
    assert result == ["b", "d", "foo"]

def test_say():
    result = say("foo")
    assert "foo" in result

def test_dict_say_boost():
    d = {"environment":"live","id":235808,"id_subclaim":"39c4de7413651ced5b4feb4afb3f577f","sessions":[{"created":"2024-12-18T11:37:34.000000+01:00","id":596029,"messages":[{"id":106161,"original_question":"Hvor mye koster en billett?"},{"content":[{"text":"Det koster 100 kr","type":"text"},{"text":"Vil du kjøpe billett?","type":"text"}],"id":706953},{"id":887461,"original_question":"Kanskje"}]}]}
    keys = {"original_question", "text"}

    result = say_dicts(d["sessions"], keys)
    assert "billett" in result
    assert len(result) > 1