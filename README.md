# Google Cast til HomewrightHQ

En ekstern integration til [HomewrightHQ](https://github.com/magnusdunker/homewrighthq), der styrer Chromecasts, Google- og Nest-højttalere og -skærme, Android TV og Cast-grupper som medieafspillere. Den taler Cast-protokollen direkte med enhederne på det lokale netværk, uden om Googles cloud.

## Installation

I HomewrightHQ: **Indstillinger → Integrationer → Fra GitHub**. Indsæt

```
https://github.com/magnusdunker/homewrighthq-google-cast
```

og tryk **Installér**. Tilføj derefter **Google Cast** under **Tilføj integration**. Formularen viser de enheder, der blev fundet på netværket. Står alle valgt, kommer enheder, der dukker op senere, også med. Når integrationen er installeret, foreslår HomewrightHQ den også selv, når den finder en Cast-enhed.

Der er kun én opsætning, og den dækker alle enheder. Valget af enheder kan ændres under integrationens indstillinger.

### Enheder, som mDNS ikke finder

Enhederne findes med mDNS (`_googlecast._tcp`), som kun virker på samme netværk. Står en enhed på et andet VLAN, eller kører HomewrightHQ i Docker uden host-netværk, så skriv dens adresse under **Ekstra adresser**, f.eks. `192.168.20.15` eller `tv.lan:8009`. Flere adresser adskilles med komma. Adressen prøves, når du gemmer, og integrationen prøver igen hvert 30. sekund, hvis enheden er væk ved opstart.

## Hvad du får

Hver enhed bliver en enhed i HomewrightHQ med én medieafspiller. Navnet er det, enheden har i Google Home-appen.

Tilstanden er:

- **off**: der kører ingen app, eller enheden viser sin pauseskærm
- **idle**: en app kører, men afspiller ikke noget
- **playing**, **paused** og **buffering** efter mediets tilstand

Attributterne følger Home Assistant: `volume_level`, `is_volume_muted`, `app_id`, `app_name`, `media_title`, `media_artist`, `media_album_name`, `media_series_title`, `media_content_id`, `media_content_type`, `media_duration`, `media_position`, `media_position_updated_at`, `entity_picture` og `supported_features`.

Tjenesterne er:

- `turn_on` starter Default Media Receiver, og `turn_off` stopper den app, der kører
- `volume_set`, `volume_mute`, `volume_up` og `volume_down`
- `media_play`, `media_pause`, `media_play_pause`, `media_stop` og `media_seek`
- `media_next_track` og `media_previous_track`, når appen har en kø
- `play_media` afspiller en URL i Default Media Receiver:

```yaml
action: media_player.play_media
target:
  entity_id: media_player.stue
data:
  media_content_id: http://nas.lan/musik/sang.mp3
  media_content_type: music        # eller en MIME-type som audio/mpeg
  extra:
    title: Sang
    thumb: http://nas.lan/musik/cover.jpg
    stream_type: LIVE              # til radio og andre livestreams
```

Er `media_content_type` ikke en MIME-type, gættes den ud fra URL'ens endelse. Tjenesten svarer først, når enheden har indlæst mediet, og fejler, hvis enheden ikke kan afspille det.

Integrationen holder en forbindelse åben til hver enhed, og enhederne sender selv deres tilstand, når den ændrer sig. Mistes forbindelsen, bliver afspilleren utilgængelig, og integrationen forbinder igen efter 1 sekund og derefter med længere pauser op til 30 sekunder. Skifter enheden IP-adresse, opdager mDNS det.

Integrationen husker den adresse, mDNS sidst gav hver enhed, og forbinder til den ved opstart. Så kommer en enhed med, selvom den ikke svarer på mDNS, når HomewrightHQ starter. Den spørger også på mDNS igen hvert 5. minut, så en enhed, der først svarer senere, bliver fundet.

## Udvikling

Bygget i Rust på SDK'en `thehome-plugin` fra [thehome-example-integration-rust](https://github.com/magnusdunker/homewrighthq-example-integration-rust).

- `src/proto.rs`: Cast-beskeder (protobuf) og rammerne omkring dem
- `src/conn.rs`: TLS til enhederne, som har selvsignerede certifikater
- `src/device.rs`: forbindelsen til én enhed, tjenesterne og genforbindelse
- `src/status.rs`: fra enhedens status til tilstand og attributter
- `src/discovery.rs`: mDNS og enhedens `eureka_info`-side
- `src/lib.rs`: opsætningen og forbindelsen til HomewrightHQ
- `tests/`: tests mod en falsk Cast-enhed i `tests/fake` og en falsk HomewrightHQ i `tests/host`

```
cargo test
cargo build --release
```

En ny version udgives med et tag, f.eks. `git tag v0.2.0 && git push origin v0.2.0`. Workflowet bygger til Windows, Linux (x86_64 og arm64, statisk musl) og macOS og lægger binærerne på releasen.

## Licens

MIT
