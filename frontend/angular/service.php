<?php

  $urlParams = explode('?', $_SERVER['REQUEST_URI']);
  $functionName = $urlParams[2];
  //print $functionName;

  $curl_h = curl_init('http://104.196.37.110:20550/slafka_daily/*');

  curl_setopt($curl_h, CURLOPT_HTTPHEADER,
    array('Accept: application/json')
  );

  # do not output, but store to variable
  curl_setopt($curl_h, CURLOPT_RETURNTRANSFER, true);

  $response = json_decode(curl_exec($curl_h));
  $counter = 0;
  foreach($response->Row as $row) {
      //print_r($row);
      $output_obj[$counter]['messageDate'] = base64_decode($row->key);
      foreach($row->Cell as $cell) {
        $output_obj[$counter][str_replace('tsa:','',base64_decode($cell->column))] = base64_decode($cell->{'$'});
      }
      $counter++;
  }
  print_r(json_encode($output_obj));

  /*print '
[
  {"messageDate":"2015-12-17","latestTimestamp":"2015-12-27T21:31:20:123Z","totalMsgs":"28","totalSentiment":"115","uniqueUsers":"3"},
  {"messageDate":"2015-12-18","latestTimestamp":"2015-12-18T22:35:30:123Z","totalMsgs":"15","totalSentiment":"15","uniqueUsers":"2"},
  {"messageDate":"2015-12-19","latestTimestamp":"2015-12-19T22:35:30:123Z","totalMsgs":"18","totalSentiment":"11","uniqueUsers":"6"},
  {"messageDate":"2015-12-20","latestTimestamp":"2015-12-20T16:40:30:123Z","totalMsgs":"22","totalSentiment":"16","uniqueUsers":"8"},
  {"messageDate":"2015-12-21","latestTimestamp":"2015-12-21T22:35:30:123Z","totalMsgs":"26","totalSentiment":"115","uniqueUsers":"3"}
]';*/
?>
