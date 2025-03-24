{% docs competitor_id %}
    
| event id          | role                                             |
|-------------------|--------------------------------------------------|
| sport event table | primary key                                      |
| competitor table  | foreign key                                      |

{% enddocs %}

{% docs referee_id %}
    
| event id                     | role                                             |
|------------------------------|---------------------------------------|
| sport event table            | primary key                                      |
| sport event conitions table  | foreign key                                      |

{% enddocs %}

{% docs venue_id %}
    
| event id                     | role                                             |
|------------------------------|---------------------------------------|
| sport event table            | primary key                                      |
| venue table                  | foreign key                                      |

{% enddocs %}

{% docs player_id %}
    
| event id                     | role                                             |
|------------------------------|---------------------------------------|
| sport event table            | primary key                                      |
| player stats table           | foreign key                                      |

{% enddocs %}