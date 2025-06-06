CREATE OR ALTER FUNCTION dbo.ProperCase (@Text NVARCHAR(MAX))
RETURNS NVARCHAR(MAX)
AS
BEGIN
    DECLARE @Index INT = 1,
            @Char NVARCHAR(1),
            @Output NVARCHAR(MAX) = '',
            @IsNewWord BIT = 1;

    WHILE @Index <= LEN(@Text)
    BEGIN
        SET @Char = SUBSTRING(@Text, @Index, 1)
        
        IF @IsNewWord = 1
        BEGIN
            SET @Output += UPPER(@Char)
            SET @IsNewWord = 0
        END
        ELSE
            SET @Output += LOWER(@Char)

        IF @Char LIKE '[^A-Za-z0-9]' OR @Char = '_'
            SET @IsNewWord = 1

        SET @Index += 1
    END

    RETURN @Output
