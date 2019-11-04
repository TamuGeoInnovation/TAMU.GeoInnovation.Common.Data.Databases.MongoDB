using System;
using USC.GISResearchLab.Common.Core.Databases;
using USC.GISResearchLab.Common.Databases.TypeConverters;

namespace TAMU.GeoInnovation.Common.Utils.Databases.MongoDB
{
    public class MongoDBTypeConverter : AbstractDatabaseDataProviderTypeConverterManager
    {

        #region TypeNames
        public static string TYPENAME_BigInt = "BigInt";
        public static string TYPENAME_Binary = "Binary";
        public static string TYPENAME_Bit = "Bit";
        public static string TYPENAME_Blob = "Blob";
        public static string TYPENAME_Byte = "Byte";
        public static string TYPENAME_Char = "Char";
        public static string TYPENAME_Date = "Date";
        public static string TYPENAME_Datetime = "Datetime";
        public static string TYPENAME_Decimal = "Decimal";
        public static string TYPENAME_Double = "Double";
        public static string TYPENAME_Enum = "Enum";
        public static string TYPENAME_Float = "Float";
        public static string TYPENAME_Geometry = "Geometry";
        public static string TYPENAME_Int = "Int";
        public static string TYPENAME_Int16 = "Int16";
        public static string TYPENAME_Int24 = "Int24";
        public static string TYPENAME_Int32 = "Int32";
        public static string TYPENAME_Int64 = "Int64";
        public static string TYPENAME_LongText = "LongText";
        public static string TYPENAME_LongBlob = "LongBlob";
        public static string TYPENAME_MediumBlob = "MediumBlob";
        public static string TYPENAME_MediumText = "MediumText";
        public static string TYPENAME_Newdate = "Newdate";
        public static string TYPENAME_NewDecimal = "NewDecimal";
        public static string TYPENAME_Set = "Set";
        public static string TYPENAME_String = "String";
        public static string TYPENAME_Text = "Text";
        public static string TYPENAME_Time = "Time";
        public static string TYPENAME_Timestamp = "Timestamp";
        public static string TYPENAME_TinyBlob = "TinyBlob";
        public static string TYPENAME_TinyText = "TinyText";
        public static string TYPENAME_UByte = "UByte";
        public static string TYPENAME_UInt16 = "UInt16";
        public static string TYPENAME_UInt24 = "UInt24";
        public static string TYPENAME_UInt32 = "UInt32";
        public static string TYPENAME_UInt64 = "UInt64";
        public static string TYPENAME_VarBinary = "VarBinary";
        public static string TYPENAME_VarChar = "VarChar";
        public static string TYPENAME_VarString = "VarString";
        public static string TYPENAME_Year = "Year";
        #endregion

        public MongoDBTypeConverter()
        {
            DatabaseType = DatabaseType.MongoDB;
            DataProviderType = DataProviderType.MongoDB;
        }

        public override int GetDefaultLength(DatabaseSuperDataType type)
        {
            throw new NotImplementedException();
        }


        public override int GetDefaultPrecision(DatabaseSuperDataType type)
        {
            throw new NotImplementedException();
        }

        public override DatabaseSuperDataType ToSuperType(object dbType)
        {
            throw new NotImplementedException("There is no DataType for MongoDB");
            //DatabaseSuperDataType ret;
            //MySqlDbType type = (MySqlDbType)dbType;

            //switch (type)
            //{
            //    case MySqlDbType.Binary:
            //        ret = DatabaseSuperDataType.Binary;
            //        break;
            //    case MySqlDbType.Bit:
            //        ret = DatabaseSuperDataType.Bit;
            //        break;
            //    case MySqlDbType.Blob:
            //        ret = DatabaseSuperDataType.Blob;
            //        break;
            //    case MySqlDbType.Date:
            //        ret = DatabaseSuperDataType.Date;
            //        break;
            //    case MySqlDbType.Datetime:
            //        ret = DatabaseSuperDataType.DateTime;
            //        break;
            //    case MySqlDbType.Decimal:
            //        ret = DatabaseSuperDataType.Decimal;
            //        break;
            //    case MySqlDbType.Float:
            //        ret = DatabaseSuperDataType.Float;
            //        break;
            //    case MySqlDbType.Geometry:
            //        ret = DatabaseSuperDataType.Geometry;
            //        break;
            //    case MySqlDbType.Int16:
            //        ret = DatabaseSuperDataType.Int16;
            //        break;
            //    case MySqlDbType.Int24:
            //        ret = DatabaseSuperDataType.Int24;
            //        break;
            //    case MySqlDbType.Int32:
            //        ret = DatabaseSuperDataType.Int32;
            //        break;
            //    case MySqlDbType.Int64:
            //        ret = DatabaseSuperDataType.Int64;
            //        break;
            //    case MySqlDbType.MediumBlob:
            //        ret = DatabaseSuperDataType.MediumBlob;
            //        break;
            //    case MySqlDbType.Newdate:
            //        ret = DatabaseSuperDataType.Newdate;
            //        break;
            //    case MySqlDbType.NewDecimal:
            //        ret = DatabaseSuperDataType.NewDecimal;
            //        break;
            //    case MySqlDbType.Set:
            //        ret = DatabaseSuperDataType.Set;
            //        break;
            //    case MySqlDbType.String:
            //        ret = DatabaseSuperDataType.String;
            //        break;
            //    case MySqlDbType.Text:
            //        ret = DatabaseSuperDataType.Text;
            //        break;
            //    case MySqlDbType.Time:
            //        ret = DatabaseSuperDataType.Time;
            //        break;
            //    case MySqlDbType.Timestamp:
            //        ret = DatabaseSuperDataType.Timestamp;
            //        break;
            //    case MySqlDbType.TinyBlob:
            //        ret = DatabaseSuperDataType.TinyBlob;
            //        break;
            //    case MySqlDbType.TinyText:
            //        ret = DatabaseSuperDataType.TinyText;
            //        break;
            //    case MySqlDbType.UByte:
            //        ret = DatabaseSuperDataType.UByte;
            //        break;
            //    case MySqlDbType.UInt16:
            //        ret = DatabaseSuperDataType.UInt16;
            //        break;
            //    case MySqlDbType.UInt24:
            //        ret = DatabaseSuperDataType.UInt24;
            //        break;
            //    case MySqlDbType.UInt32:
            //        ret = DatabaseSuperDataType.UInt32;
            //        break;
            //    case MySqlDbType.UInt64:
            //        ret = DatabaseSuperDataType.UInt64;
            //        break;
            //    case MySqlDbType.VarBinary:
            //        ret = DatabaseSuperDataType.VarBinary;
            //        break;
            //    case MySqlDbType.VarChar:
            //        ret = DatabaseSuperDataType.VarChar;
            //        break;
            //    case MySqlDbType.VarString:
            //        ret = DatabaseSuperDataType.VarString;
            //        break;
            //    case MySqlDbType.Year:
            //        ret = DatabaseSuperDataType.Year;
            //        break;
            //    default:
            //        throw new Exception("Unexpected type: " + type);
            //}
            //return ret;
        }


        public void FromShowColumnsString(string type)
        {
            throw new NotImplementedException("There is no DataType for MongoDB");

            //{
            //    MySqlDbType ret = MySqlDbType.Text;

            //    if (String.Compare(type, TYPENAME_BigInt, true) == 0)
            //    {
            //        ret = MySqlDbType.UInt64;
            //    }
            //    else if (String.Compare(type, TYPENAME_Binary, true) == 0)
            //    {
            //        ret = MySqlDbType.Binary;
            //    }
            //    else if (String.Compare(type, TYPENAME_Bit, true) == 0)
            //    {
            //        ret = MySqlDbType.Bit;
            //    }
            //    else if (String.Compare(type, TYPENAME_Blob, true) == 0)
            //    {
            //        ret = MySqlDbType.Blob;
            //    }
            //    else if (String.Compare(type, TYPENAME_Byte, true) == 0)
            //    {
            //        ret = MySqlDbType.Byte;
            //    }
            //    else if (String.Compare(type, TYPENAME_Char, true) == 0)
            //    {
            //        ret = MySqlDbType.VarChar;
            //    }
            //    else if (String.Compare(type, TYPENAME_Date, true) == 0)
            //    {
            //        ret = MySqlDbType.Date;
            //    }
            //    else if (String.Compare(type, TYPENAME_Datetime, true) == 0)
            //    {
            //        ret = MySqlDbType.Datetime;
            //    }
            //    else if (String.Compare(type, TYPENAME_Decimal, true) == 0)
            //    {
            //        ret = MySqlDbType.Decimal;
            //    }
            //    else if (String.Compare(type, TYPENAME_Double, true) == 0)
            //    {
            //        ret = MySqlDbType.Double;
            //    }
            //    else if (String.Compare(type, TYPENAME_Enum, true) == 0)
            //    {
            //        ret = MySqlDbType.Enum;
            //    }
            //    else if (String.Compare(type, TYPENAME_Float, true) == 0)
            //    {
            //        ret = MySqlDbType.Float;
            //    }
            //    else if (String.Compare(type, TYPENAME_Geometry, true) == 0)
            //    {
            //        ret = MySqlDbType.Geometry;
            //    }
            //    else if (String.Compare(type, TYPENAME_Int, true) == 0)
            //    {
            //        ret = MySqlDbType.Int32;
            //    }
            //    else if (String.Compare(type, TYPENAME_Int16, true) == 0)
            //    {
            //        ret = MySqlDbType.Int16;
            //    }
            //    else if (String.Compare(type, TYPENAME_Int24, true) == 0)
            //    {
            //        ret = MySqlDbType.Int24;
            //    }
            //    else if (String.Compare(type, TYPENAME_Int32, true) == 0)
            //    {
            //        ret = MySqlDbType.Int32;
            //    }
            //    else if (String.Compare(type, TYPENAME_Int64, true) == 0)
            //    {
            //        ret = MySqlDbType.Int64;
            //    }
            //    else if (String.Compare(type, TYPENAME_LongBlob, true) == 0)
            //    {
            //        ret = MySqlDbType.LongBlob;
            //    }
            //    else if (String.Compare(type, TYPENAME_LongText, true) == 0)
            //    {
            //        ret = MySqlDbType.LongText;
            //    }
            //    else if (String.Compare(type, TYPENAME_MediumBlob, true) == 0)
            //    {
            //        ret = MySqlDbType.MediumBlob;
            //    }
            //    else if (String.Compare(type, TYPENAME_MediumText, true) == 0)
            //    {
            //        ret = MySqlDbType.MediumText;
            //    }
            //    else if (String.Compare(type, TYPENAME_Newdate, true) == 0)
            //    {
            //        ret = MySqlDbType.Newdate;
            //    }
            //    else if (String.Compare(type, TYPENAME_NewDecimal, true) == 0)
            //    {
            //        ret = MySqlDbType.NewDecimal;
            //    }
            //    else if (String.Compare(type, TYPENAME_Set, true) == 0)
            //    {
            //        ret = MySqlDbType.Set;
            //    }
            //    else if (String.Compare(type, TYPENAME_String, true) == 0)
            //    {
            //        ret = MySqlDbType.String;
            //    }
            //    else if (String.Compare(type, TYPENAME_Text, true) == 0)
            //    {
            //        ret = MySqlDbType.Text;
            //    }
            //    else if (String.Compare(type, TYPENAME_Time, true) == 0)
            //    {
            //        ret = MySqlDbType.Time;
            //    }
            //    else if (String.Compare(type, TYPENAME_Timestamp, true) == 0)
            //    {
            //        ret = MySqlDbType.Timestamp;
            //    }
            //    else if (String.Compare(type, TYPENAME_TinyBlob, true) == 0)
            //    {
            //        ret = MySqlDbType.TinyBlob;
            //    }
            //    else if (String.Compare(type, TYPENAME_TinyText, true) == 0)
            //    {
            //        ret = MySqlDbType.TinyText;
            //    }
            //    else if (String.Compare(type, TYPENAME_UByte, true) == 0)
            //    {
            //        ret = MySqlDbType.UByte;
            //    }
            //    else if (String.Compare(type, TYPENAME_UInt16, true) == 0)
            //    {
            //        ret = MySqlDbType.UInt16;
            //    }
            //    else if (String.Compare(type, TYPENAME_UInt24, true) == 0)
            //    {
            //        ret = MySqlDbType.UInt24;
            //    }
            //    else if (String.Compare(type, TYPENAME_UInt32, true) == 0)
            //    {
            //        ret = MySqlDbType.UInt32;
            //    }
            //    else if (String.Compare(type, TYPENAME_UInt64, true) == 0)
            //    {
            //        ret = MySqlDbType.UInt64;
            //    }
            //    else if (String.Compare(type, TYPENAME_VarBinary, true) == 0)
            //    {
            //        ret = MySqlDbType.VarBinary;
            //    }
            //    else if (String.Compare(type, TYPENAME_VarChar, true) == 0)
            //    {
            //        ret = MySqlDbType.VarChar;
            //    }
            //    else if (String.Compare(type, TYPENAME_VarString, true) == 0)
            //    {
            //        ret = MySqlDbType.VarString;
            //    }
            //    else if (String.Compare(type, TYPENAME_Year, true) == 0)
            //    {
            //        ret = MySqlDbType.Year;
            //    }
            //    else
            //    {
            //        throw new Exception("Unexpected or unimplemented FromShowColumnsString: " + type);
            //    }

            //    return ret;
        }

        public override object FromDatabaseSuperDataType(DatabaseSuperDataType type)
        {
            throw new NotImplementedException("There is no DataType for MongoDB");
            //MySqlDbType ret = MySqlDbType.Text;

            //switch (type)
            //{
            //    case DatabaseSuperDataType.BigInt:
            //        ret = MySqlDbType.Int64;
            //        break;
            //    case DatabaseSuperDataType.Binary:
            //        ret = MySqlDbType.Binary;
            //        break;
            //    case DatabaseSuperDataType.Bit:
            //        ret = MySqlDbType.Bit;
            //        break;
            //    case DatabaseSuperDataType.Blob:
            //        ret = MySqlDbType.Blob;
            //        break;
            //    case DatabaseSuperDataType.Boolean:
            //        ret = MySqlDbType.Bit;
            //        break;
            //    case DatabaseSuperDataType.BSTR:
            //        ret = MySqlDbType.VarChar;
            //        break;
            //    case DatabaseSuperDataType.Char:
            //        ret = MySqlDbType.VarChar;
            //        break;
            //    case DatabaseSuperDataType.Counter:
            //        ret = MySqlDbType.Int32;
            //        break;
            //    case DatabaseSuperDataType.Currency:
            //        ret = MySqlDbType.Decimal;
            //        break;
            //    case DatabaseSuperDataType.Date:
            //        ret = MySqlDbType.Date;
            //        break;
            //    case DatabaseSuperDataType.DateTime:
            //        ret = MySqlDbType.Datetime;
            //        break;
            //    case DatabaseSuperDataType.DateTime2:
            //        ret = MySqlDbType.Datetime;
            //        break;
            //    case DatabaseSuperDataType.DateTimeOffset:
            //        ret = MySqlDbType.Datetime;
            //        break;
            //    case DatabaseSuperDataType.DBDate:
            //        ret = MySqlDbType.Date;
            //        break;
            //    case DatabaseSuperDataType.DBTime:
            //        ret = MySqlDbType.Time;
            //        break;
            //    case DatabaseSuperDataType.DBTimeStamp:
            //        ret = MySqlDbType.Timestamp;
            //        break;
            //    case DatabaseSuperDataType.Decimal:
            //        ret = MySqlDbType.Decimal;
            //        break;
            //    case DatabaseSuperDataType.Double:
            //        ret = MySqlDbType.Double;
            //        break;
            //    case DatabaseSuperDataType.Empty:
            //        ret = MySqlDbType.Text;
            //        break;
            //    case DatabaseSuperDataType.Error:
            //        ret = MySqlDbType.Text;
            //        break;
            //    case DatabaseSuperDataType.Filetime:
            //        ret = MySqlDbType.Datetime;
            //        break;
            //    case DatabaseSuperDataType.Float:
            //        ret = MySqlDbType.Float;
            //        break;
            //    case DatabaseSuperDataType.Geometry:
            //        ret = MySqlDbType.Geometry;
            //        break;
            //    case DatabaseSuperDataType.Guid:
            //        ret = MySqlDbType.Text;
            //        break;
            //    case DatabaseSuperDataType.IDispatch:
            //        ret = MySqlDbType.Text;
            //        break;
            //    case DatabaseSuperDataType.Image:
            //        ret = MySqlDbType.Blob;
            //        break;
            //    case DatabaseSuperDataType.Int16:
            //        ret = MySqlDbType.Int16;
            //        break;
            //    case DatabaseSuperDataType.Int24:
            //        ret = MySqlDbType.Int24;
            //        break;
            //    case DatabaseSuperDataType.Int32:
            //        ret = MySqlDbType.Int32;
            //        break;
            //    case DatabaseSuperDataType.Int64:
            //        ret = MySqlDbType.Int64;
            //        break;
            //    case DatabaseSuperDataType.IUnknown:
            //        ret = MySqlDbType.Text;
            //        break;
            //    case DatabaseSuperDataType.Long:
            //        ret = MySqlDbType.Int32;
            //        break;
            //    case DatabaseSuperDataType.LongBinary:
            //        ret = MySqlDbType.LongBlob;
            //        break;
            //    case DatabaseSuperDataType.LongText:
            //        ret = MySqlDbType.LongText;
            //        break;
            //    case DatabaseSuperDataType.LongVarBinary:
            //        ret = MySqlDbType.LongBlob;
            //        break;
            //    case DatabaseSuperDataType.LongVarChar:
            //        ret = MySqlDbType.LongText;
            //        break;
            //    case DatabaseSuperDataType.LongVarWChar:
            //        ret = MySqlDbType.LongText;
            //        break;
            //    case DatabaseSuperDataType.MediumBlob:
            //        ret = MySqlDbType.MediumBlob;
            //        break;
            //    case DatabaseSuperDataType.Money:
            //        ret = MySqlDbType.Decimal;
            //        break;
            //    case DatabaseSuperDataType.NChar:
            //        ret = MySqlDbType.VarChar;
            //        break;
            //    case DatabaseSuperDataType.Newdate:
            //        ret = MySqlDbType.Newdate;
            //        break;
            //    case DatabaseSuperDataType.NewDecimal:
            //        ret = MySqlDbType.NewDecimal;
            //        break;
            //    case DatabaseSuperDataType.NText:
            //        ret = MySqlDbType.Text;
            //        break;
            //    case DatabaseSuperDataType.Numeric:
            //        ret = MySqlDbType.Decimal;
            //        break;
            //    case DatabaseSuperDataType.NVarChar:
            //        ret = MySqlDbType.VarString;
            //        break;
            //    case DatabaseSuperDataType.PropVariant:
            //        ret = MySqlDbType.Text;
            //        break;
            //    case DatabaseSuperDataType.Real:
            //        ret = MySqlDbType.Decimal;
            //        break;
            //    case DatabaseSuperDataType.Set:
            //        ret = MySqlDbType.Text;
            //        break;
            //    case DatabaseSuperDataType.Short:
            //        ret = MySqlDbType.Int16;
            //        break;
            //    case DatabaseSuperDataType.Single:
            //        ret = MySqlDbType.Decimal;
            //        break;
            //    case DatabaseSuperDataType.SmallDateTime:
            //        ret = MySqlDbType.Datetime;
            //        break;
            //    case DatabaseSuperDataType.SmallInt:
            //        ret = MySqlDbType.Int16;
            //        break;
            //    case DatabaseSuperDataType.SmallMoney:
            //        ret = MySqlDbType.Decimal;
            //        break;
            //    case DatabaseSuperDataType.String:
            //        ret = MySqlDbType.String;
            //        break;
            //    case DatabaseSuperDataType.Structured:
            //        ret = MySqlDbType.Text;
            //        break;
            //    case DatabaseSuperDataType.Text:
            //        ret = MySqlDbType.Text;
            //        break;
            //    case DatabaseSuperDataType.Time:
            //        ret = MySqlDbType.Time;
            //        break;
            //    case DatabaseSuperDataType.Timestamp:
            //        ret = MySqlDbType.Timestamp;
            //        break;
            //    case DatabaseSuperDataType.TinyBlob:
            //        ret = MySqlDbType.TinyBlob;
            //        break;
            //    case DatabaseSuperDataType.TinyInt:
            //        ret = MySqlDbType.Int16;
            //        break;
            //    case DatabaseSuperDataType.TinyText:
            //        ret = MySqlDbType.TinyText;
            //        break;
            //    case DatabaseSuperDataType.UByte:
            //        ret = MySqlDbType.UByte;
            //        break;
            //    case DatabaseSuperDataType.Udt:
            //        ret = MySqlDbType.Text;
            //        break;
            //    case DatabaseSuperDataType.UInt16:
            //        ret = MySqlDbType.UInt16;
            //        break;
            //    case DatabaseSuperDataType.UInt24:
            //        ret = MySqlDbType.UInt24;
            //        break;
            //    case DatabaseSuperDataType.UInt32:
            //        ret = MySqlDbType.UInt32;
            //        break;
            //    case DatabaseSuperDataType.UInt64:
            //        ret = MySqlDbType.UInt64;
            //        break;
            //    case DatabaseSuperDataType.UniqueIdentifier:
            //        ret = MySqlDbType.Text;
            //        break;
            //    case DatabaseSuperDataType.UnsignedBigInt:
            //        ret = MySqlDbType.UInt64;
            //        break;
            //    case DatabaseSuperDataType.UnsignedInt:
            //        ret = MySqlDbType.UInt32;
            //        break;
            //    case DatabaseSuperDataType.UnsignedSmallInt:
            //        ret = MySqlDbType.UInt24;
            //        break;
            //    case DatabaseSuperDataType.UnsignedTinyInt:
            //        ret = MySqlDbType.UInt16;
            //        break;
            //    case DatabaseSuperDataType.VarBinary:
            //        ret = MySqlDbType.VarBinary;
            //        break;
            //    case DatabaseSuperDataType.VarChar:
            //        ret = MySqlDbType.VarChar;
            //        break;
            //    case DatabaseSuperDataType.Variant:
            //        ret = MySqlDbType.Text;
            //        break;
            //    case DatabaseSuperDataType.VarNumeric:
            //        ret = MySqlDbType.Decimal;
            //        break;
            //    case DatabaseSuperDataType.VarString:
            //        ret = MySqlDbType.VarString;
            //        break;
            //    case DatabaseSuperDataType.VarWChar:
            //        ret = MySqlDbType.VarChar;
            //        break;
            //    case DatabaseSuperDataType.WChar:
            //        ret = MySqlDbType.VarChar;
            //        break;
            //    case DatabaseSuperDataType.Xml:
            //        ret = MySqlDbType.Text;
            //        break;
            //    case DatabaseSuperDataType.Year:
            //        ret = MySqlDbType.Year;
            //        break;
            //    default:
            //        throw new Exception("Unexpected or unimplemented DatabaseSuperDataType: " + type);
            //}

            //return ret;
        }

        public override Type GetSystemType(object dbType)
        {
            throw new NotImplementedException("There is no DataType for MongoDB");
            //Type ret = null;
            //MySqlDbType type = (MySqlDbType)dbType;

            //switch (type)
            //{
            //    case MySqlDbType.Binary:
            //        ret = typeof(Byte[]);
            //        break;
            //    case MySqlDbType.Bit:
            //        ret = typeof(Boolean);
            //        break;
            //    case MySqlDbType.Blob:
            //        ret = typeof(Byte[]);
            //        break;
            //    case MySqlDbType.Date:
            //        ret = typeof(DateTime);
            //        break;
            //    case MySqlDbType.Datetime:
            //        ret = typeof(DateTime);
            //        break;
            //    case MySqlDbType.Decimal:
            //        ret = typeof(Decimal);
            //        break;
            //    case MySqlDbType.Float:
            //        ret = typeof(Double);
            //        break;
            //    case MySqlDbType.Geometry:
            //        ret = typeof(String);
            //        break;
            //    case MySqlDbType.Int16:
            //        ret = typeof(Int16);
            //        break;
            //    case MySqlDbType.Int24:
            //        ret = typeof(Int32);
            //        break;
            //    case MySqlDbType.Int32:
            //        ret = typeof(Int32);
            //        break;
            //    case MySqlDbType.Int64:
            //        ret = typeof(Int64);
            //        break;
            //    case MySqlDbType.MediumBlob:
            //        ret = typeof(Byte[]);
            //        break;
            //    case MySqlDbType.Newdate:
            //        ret = typeof(DateTime);
            //        break;
            //    case MySqlDbType.NewDecimal:
            //        ret = typeof(Decimal);
            //        break;
            //    case MySqlDbType.Set:
            //        ret = typeof(Object[]);
            //        break;
            //    case MySqlDbType.String:
            //        ret = typeof(String);
            //        break;
            //    case MySqlDbType.Text:
            //        ret = typeof(String);
            //        break;
            //    case MySqlDbType.Time:
            //        ret = typeof(DateTime);
            //        break;
            //    case MySqlDbType.Timestamp:
            //        ret = typeof(DateTime);
            //        break;
            //    case MySqlDbType.TinyBlob:
            //        ret = typeof(Byte[]);
            //        break;
            //    case MySqlDbType.TinyText:
            //        ret = typeof(String);
            //        break;
            //    case MySqlDbType.UByte:
            //        ret = typeof(Byte);
            //        break;
            //    case MySqlDbType.UInt16:
            //        ret = typeof(UInt16);
            //        break;
            //    case MySqlDbType.UInt24:
            //        ret = typeof(UInt32);
            //        break;
            //    case MySqlDbType.UInt32:
            //        ret = typeof(UInt32);
            //        break;
            //    case MySqlDbType.UInt64:
            //        ret = typeof(UInt64);
            //        break;
            //    case MySqlDbType.VarBinary:
            //        ret = typeof(Byte[]);
            //        break;
            //    case MySqlDbType.VarChar:
            //        ret = typeof(String);
            //        break;
            //    case MySqlDbType.VarString:
            //        ret = typeof(String);
            //        break;
            //    case MySqlDbType.Year:
            //        ret = typeof(Int32);
            //        break;
            //    default:
            //        throw new Exception("Unexpected type: " + type);
            //}
            //return ret;
        }

        public override object ConvertType(object dbSuperType, DatabaseType databaseType)
        {
            throw new NotImplementedException();
        }

        public override string GetTypeAsString(DatabaseSuperDataType type)
        {
            throw new NotImplementedException("There is no DataType for MongoDB");
            //string ret = null;

            //switch (type)
            //{
            //    case DatabaseSuperDataType.BigInt:
            //        ret = TYPENAME_Int64;
            //        break;
            //    case DatabaseSuperDataType.Binary:
            //        ret = TYPENAME_Binary;
            //        break;
            //    case DatabaseSuperDataType.Bit:
            //        ret = TYPENAME_Bit;
            //        break;
            //    case DatabaseSuperDataType.Blob:
            //        ret = TYPENAME_Blob;
            //        break;
            //    case DatabaseSuperDataType.Boolean:
            //        ret = TYPENAME_Bit;
            //        break;
            //    case DatabaseSuperDataType.BSTR:
            //        ret = TYPENAME_VarChar;
            //        break;
            //    case DatabaseSuperDataType.Char:
            //        ret = TYPENAME_VarChar;
            //        break;
            //    case DatabaseSuperDataType.Counter:
            //        ret = TYPENAME_Int32;
            //        break;
            //    case DatabaseSuperDataType.Currency:
            //        ret = TYPENAME_Decimal;
            //        break;
            //    case DatabaseSuperDataType.Date:
            //        ret = TYPENAME_Date;
            //        break;
            //    case DatabaseSuperDataType.DateTime:
            //        ret = TYPENAME_Datetime;
            //        break;
            //    case DatabaseSuperDataType.DateTime2:
            //        ret = TYPENAME_Datetime;
            //        break;
            //    case DatabaseSuperDataType.DateTimeOffset:
            //        ret = TYPENAME_Datetime;
            //        break;
            //    case DatabaseSuperDataType.DBDate:
            //        ret = TYPENAME_Date;
            //        break;
            //    case DatabaseSuperDataType.DBTime:
            //        ret = TYPENAME_Time;
            //        break;
            //    case DatabaseSuperDataType.DBTimeStamp:
            //        ret = TYPENAME_Timestamp;
            //        break;
            //    case DatabaseSuperDataType.Decimal:
            //        ret = TYPENAME_Decimal;
            //        break;
            //    case DatabaseSuperDataType.Double:
            //        ret = TYPENAME_Double;
            //        break;
            //    case DatabaseSuperDataType.Empty:
            //        ret = TYPENAME_Text;
            //        break;
            //    case DatabaseSuperDataType.Error:
            //        ret = TYPENAME_Text;
            //        break;
            //    case DatabaseSuperDataType.Filetime:
            //        ret = TYPENAME_Datetime;
            //        break;
            //    case DatabaseSuperDataType.Float:
            //        ret = TYPENAME_Float;
            //        break;
            //    case DatabaseSuperDataType.Geography:
            //        ret = TYPENAME_Geometry;
            //        break;
            //    case DatabaseSuperDataType.Geometry:
            //        ret = TYPENAME_Geometry;
            //        break;
            //    case DatabaseSuperDataType.Guid:
            //        ret = TYPENAME_Text;
            //        break;
            //    case DatabaseSuperDataType.IDispatch:
            //        ret = TYPENAME_Text;
            //        break;
            //    case DatabaseSuperDataType.Image:
            //        ret = TYPENAME_Blob;
            //        break;
            //    case DatabaseSuperDataType.Int16:
            //        ret = TYPENAME_Int16;
            //        break;
            //    case DatabaseSuperDataType.Int24:
            //        ret = TYPENAME_Int24;
            //        break;
            //    case DatabaseSuperDataType.Int32:
            //        ret = TYPENAME_Int32;
            //        break;
            //    case DatabaseSuperDataType.Int64:
            //        ret = TYPENAME_Int64;
            //        break;
            //    case DatabaseSuperDataType.IUnknown:
            //        ret = TYPENAME_Text;
            //        break;
            //    case DatabaseSuperDataType.Long:
            //        ret = TYPENAME_Int32;
            //        break;
            //    case DatabaseSuperDataType.LongBinary:
            //        ret = TYPENAME_Blob;
            //        break;
            //    case DatabaseSuperDataType.LongText:
            //        ret = TYPENAME_LongText;
            //        break;
            //    case DatabaseSuperDataType.LongVarBinary:
            //        ret = TYPENAME_LongBlob;
            //        break;
            //    case DatabaseSuperDataType.LongVarChar:
            //        ret = TYPENAME_LongText;
            //        break;
            //    case DatabaseSuperDataType.LongVarWChar:
            //        ret = TYPENAME_LongText;
            //        break;
            //    case DatabaseSuperDataType.MediumBlob:
            //        ret = TYPENAME_MediumBlob;
            //        break;
            //    case DatabaseSuperDataType.Money:
            //        ret = TYPENAME_Decimal;
            //        break;
            //    case DatabaseSuperDataType.NChar:
            //        ret = TYPENAME_VarChar;
            //        break;
            //    case DatabaseSuperDataType.Newdate:
            //        ret = TYPENAME_Newdate;
            //        break;
            //    case DatabaseSuperDataType.NewDecimal:
            //        ret = TYPENAME_NewDecimal;
            //        break;
            //    case DatabaseSuperDataType.NText:
            //        ret = TYPENAME_Text;
            //        break;
            //    case DatabaseSuperDataType.Numeric:
            //        ret = TYPENAME_Decimal;
            //        break;
            //    case DatabaseSuperDataType.NVarChar:
            //        ret = TYPENAME_VarString;
            //        break;
            //    case DatabaseSuperDataType.PropVariant:
            //        ret = TYPENAME_Text;
            //        break;
            //    case DatabaseSuperDataType.Real:
            //        ret = TYPENAME_Decimal;
            //        break;
            //    case DatabaseSuperDataType.Set:
            //        ret = TYPENAME_Text;
            //        break;
            //    case DatabaseSuperDataType.Short:
            //        ret = TYPENAME_Int16;
            //        break;
            //    case DatabaseSuperDataType.Single:
            //        ret = TYPENAME_Decimal;
            //        break;
            //    case DatabaseSuperDataType.SmallDateTime:
            //        ret = TYPENAME_Datetime;
            //        break;
            //    case DatabaseSuperDataType.SmallInt:
            //        ret = TYPENAME_Int16;
            //        break;
            //    case DatabaseSuperDataType.SmallMoney:
            //        ret = TYPENAME_Decimal;
            //        break;
            //    case DatabaseSuperDataType.String:
            //        ret = TYPENAME_VarChar;
            //        break;
            //    case DatabaseSuperDataType.Structured:
            //        ret = TYPENAME_Text;
            //        break;
            //    case DatabaseSuperDataType.Text:
            //        ret = TYPENAME_Text;
            //        break;
            //    case DatabaseSuperDataType.Time:
            //        ret = TYPENAME_Time;
            //        break;
            //    case DatabaseSuperDataType.Timestamp:
            //        ret = TYPENAME_Timestamp;
            //        break;
            //    case DatabaseSuperDataType.TinyBlob:
            //        ret = TYPENAME_TinyBlob;
            //        break;
            //    case DatabaseSuperDataType.TinyInt:
            //        ret = TYPENAME_Int16;
            //        break;
            //    case DatabaseSuperDataType.TinyText:
            //        ret = TYPENAME_TinyText;
            //        break;
            //    case DatabaseSuperDataType.UByte:
            //        ret = TYPENAME_UByte;
            //        break;
            //    case DatabaseSuperDataType.Udt:
            //        ret = TYPENAME_Text;
            //        break;
            //    case DatabaseSuperDataType.UInt16:
            //        ret = TYPENAME_UInt16;
            //        break;
            //    case DatabaseSuperDataType.UInt24:
            //        ret = TYPENAME_UInt24;
            //        break;
            //    case DatabaseSuperDataType.UInt32:
            //        ret = TYPENAME_UInt32;
            //        break;
            //    case DatabaseSuperDataType.UInt64:
            //        ret = TYPENAME_UInt64;
            //        break;
            //    case DatabaseSuperDataType.UniqueIdentifier:
            //        ret = TYPENAME_Text;
            //        break;
            //    case DatabaseSuperDataType.UnsignedBigInt:
            //        ret = TYPENAME_UInt64;
            //        break;
            //    case DatabaseSuperDataType.UnsignedInt:
            //        ret = TYPENAME_UInt32;
            //        break;
            //    case DatabaseSuperDataType.UnsignedSmallInt:
            //        ret = TYPENAME_UInt24;
            //        break;
            //    case DatabaseSuperDataType.UnsignedTinyInt:
            //        ret = TYPENAME_UInt16;
            //        break;
            //    case DatabaseSuperDataType.VarBinary:
            //        ret = TYPENAME_VarBinary;
            //        break;
            //    case DatabaseSuperDataType.VarChar:
            //        ret = TYPENAME_VarChar;
            //        break;
            //    case DatabaseSuperDataType.Variant:
            //        ret = TYPENAME_Text;
            //        break;
            //    case DatabaseSuperDataType.VarNumeric:
            //        ret = TYPENAME_Decimal;
            //        break;
            //    case DatabaseSuperDataType.VarString:
            //        ret = TYPENAME_VarString;
            //        break;
            //    case DatabaseSuperDataType.VarWChar:
            //        ret = TYPENAME_VarChar;
            //        break;
            //    case DatabaseSuperDataType.WChar:
            //        ret = TYPENAME_VarChar;
            //        break;
            //    case DatabaseSuperDataType.Xml:
            //        ret = TYPENAME_Text;
            //        break;
            //    case DatabaseSuperDataType.Year:
            //        ret = TYPENAME_Year;
            //        break;
            //    default:
            //        throw new Exception("Unexpected or unimplemented DatabaseSuperDataType: " + type);
            //}

            //return ret;
        }

        public override Type ToSystemTypeFromDbTypeString(string dbTypeString)
        {
            throw new NotImplementedException("MongoDB does not support strict DB types");
        }

        public override DatabaseSuperDataType ToSuperTypeFromdbTypeString(string dbTypeString)
        {
            throw new NotImplementedException("MongoDB does not support strict DB types");
        }
    }
}
