-- 來自《100家常小菜2》黃美鳳 著, 海濱圖書 2012 

CREATE TYPE 菜類_enum AS ENUM (
  '豬', 
  '牛', 
  '羊', 
  '雞', 
  '鴨鴿鵝',
  '蔬菜',
  '蛋',
  '豆腐',
  '魚',
  '蝦',
  '蟹',
  '貝類'  
);

CREATE TABLE 家常小菜 AS
SELECT 
  頁, 
  菜類,
  菜式,
  份量對應人數,
  string_split(材料, ';') AS 材料,
  string_split(醃料, ';') AS 醃料,
  string_split(調味, ';') AS 調味,
  string_split(芡汁, ';') AS 芡汁
FROM read_csv('Recipes.csv',
  delim=',',
  auto_detect=false,
  header=true,
  encoding='utf-8',
  columns={
    '頁': 'INTEGER',
    '菜類': '菜類_enum',
    '菜式': 'VARCHAR',
    '份量對應人數': 'DECIMAL(3,2)',
    '材料': 'VARCHAR',
    '醃料': 'VARCHAR',
    '調味': 'VARCHAR',
    '芡汁': 'VARCHAR'
  }
);
