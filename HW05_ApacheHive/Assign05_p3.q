SELECT count(*) FROM SHAKE s JOIN KINGJAMES k ON (s.word = k.word)

SELECT count(COALESCE(s.word, k.word)) FROM SHAKE s FULL OUTER JOIN KINGJAMES k ON (s.word = k.word)

SELECT count(*) FROM SHAKE s RIGHT OUTER JOIN KINGJAMES k ON (s.word = k.word) WHERE s.word IS NULL

SELECT count(*) FROM SHAKE s LEFT OUTER JOIN KINGJAMES k ON (s.word = k.word) WHERE k.word IS NULL