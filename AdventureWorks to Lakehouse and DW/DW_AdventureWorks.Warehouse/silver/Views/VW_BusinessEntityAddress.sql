-- Auto Generated (Do not modify) C2CB7B392C797B8FB7AACBAFFB775ECCDD613F7B727F4C695CF2E24B82E0ACF3

CREATE VIEW silver.VW_BusinessEntityAddress AS
Select BusinessEntityID, AddressID, AddressTypeID, rowguid, ModifiedDate,
ROW_NUMBER() over(Partition by BusinessEntityID order By ModifiedDate Desc) RNO
from Bronze_AdventureworksOLTP.Bronze.[BusinessEntityAddress]