create view bp_all_view as
    select human_index as position,
           uuid, victories, total_points, total_eliminations,
           total_placing, games_played, username,
           case games_played
               when 0 then 0
               else victories::decimal / games_played::decimal
           end as win_rate,
           case games_played
               when 0 then 0
               else total_placing::decimal / games_played::decimal
           end as placing_rate,
           case games_played
               when 0 then 0
               else total_points::decimal / games_played::decimal
           end as points_per_game
    from bp_all
    order by position;

create view bp_{{ period }}_view as
    select * from (
        select row_number() over (order by total_points desc) as position,
               *,
               case games_played
                   when 0 then 0
                   else victories::decimal / games_played::decimal
               end as win_rate,
               case games_played
                   when 0 then 0
                   else total_placing::decimal / games_played::decimal
               end as placing_rate,
               case games_played
                   when 0 then 0
                   else total_points::decimal / games_played::decimal
               end as points_per_game
        from (
            select current.uuid,
                   (current.victories - cached.victories) as victories,
                   (current.total_points - cached.total_points) as total_points,
                   (current.total_eliminations - cached.total_eliminations) as total_eliminations,
                   (current.total_placing - cached.total_placing) as total_placing,
                   (current.games_played - cached.games_played) as games_played,
                   current.username
            from bp_all current, bp_{{ period }} cached
            where current.uuid = cached.uuid
            ) windowed
        ) sorted;
