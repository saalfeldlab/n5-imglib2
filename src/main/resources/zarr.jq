def cleanPath($prefix): . | sub("^"+ $prefix + "/(?<x>\\w*)\/*\\.*.+" ; "\(.x)"; "g");

def containsExactString($query): reduce .[] as $i (false; . or ($i == $query));

def hasAllKeys($queryKeys):  keys as $keys |
    reduce ($queryKeys | .[] ) as $k (true; . and ( $keys | containsExactString($k)));

def inExact($arr): reduce ($arr | .[]) as $v ([., false]; [.[0], .[1] or ($v == .[0])]) | .[1];

def zarrArrayKeys: ["zarr_format", "shape", "chunks", "dtype", "compressor", "fill_value", "order", 
    "dimension_separator", "filters"];

def zarrGroupKeys: ["zarr_format"];

def isZarrArray: type == "object" and hasAllKeys( zarrArrayKeys );

def zarrArrayObj: zarrArrayKeys as $za | with_entries(select(.key | inExact( $za )));

def zarrArrayAttrs: zarrArrayKeys as $za | with_entries(select(.key | inExact( $za ) | not));

def zarrGroupObj: { "zarr_format" : .zarr_format };

def zarrGroupAttrs: zarrGroupKeys as $zg | with_entries(select(.key | inExact( $zg ) | not));

def zarrAttrs: . as $obj |
    if ($obj | isZarrArray) then 
    {
        ".zarray" : ($obj | zarrArrayObj),
        ".zattrs" : ($obj | zarrArrayAttrs)
    }
    else 
    {
        ".zgroup" : ($obj | zarrGroupObj),
        ".zattrs" : ($obj | zarrGroupAttrs)
    }
    end;

def hasAttributes: type == "object" and has("attributes");

def zomTreePath: ltrimstr( "/") | split("/") | map_values( ["members", . ] ) | flatten;

def zomAddMissingAttrs: walk(
    if type == "object" and has("members") and (has("attributes") | not) then 
        . + {"attributes" : {}}
    else .  end);

def zomAdd( $path; $attrs; $suffix ):
    ($path | zomTreePath | . + $suffix) as $p |
    getpath( $p ) as $currentAttrs |
    setpath( $p; $currentAttrs + $attrs );

def zomAddFiles($prefix):
    reduce inputs as $i ({};
        input_filename as $f |
        ($f | cleanPath($prefix)) as $p |
        if ($f | endswith(".zattrs")) then
            zomAdd($p; $i; ["attributes"])
        else
            zomAdd($p; $i; [])
        end) |
   zomAddMissingAttrs;

